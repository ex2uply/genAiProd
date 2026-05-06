"""Minimal Ollama HTTP client for runtime configuration."""
import json
import os
import urllib.request
from typing import Any

DEFAULT_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:latest")


def chat(prompt: str, model: str | None = None, host: str | None = None) -> str:
    """Send a chat prompt to Ollama and return the response text.

    Args:
        prompt: The user prompt text
        model: Ollama model name (default: from env or DEFAULT_MODEL)
        host: Ollama host URL (default: from env or DEFAULT_HOST)

    Returns:
        Response text from the model
    """
    m = model or DEFAULT_MODEL
    h = host or DEFAULT_HOST
    url = f"{h}/api/generate"

    payload = {
        "model": m,
        "prompt": prompt,
        "stream": False,
    }
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        return str(body.get("response", "")).strip()


def ask_alert_config(intent: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
    """Ask Ollama for alert threshold configuration.

    Args:
        intent: Natural language description of desired alert behavior
        default: Fallback values if AI is unavailable

    Returns:
        Dictionary with high_temp_threshold, low_moisture_threshold
    """
    default = default or {"high_temp_threshold": 35, "low_moisture_threshold": 40}

    if os.getenv("USE_OLLAMA_PIPELINE", "").lower() not in ("1", "true", "yes"):
        return default

    prompt = f"""You are an AI assistant configuring an agriculture monitoring system.

The user wants: {intent}

Provide thresholds as JSON with these exact keys:
- high_temp_threshold (number): Temperature above which to alert
- low_moisture_threshold (number): Moisture below which to alert

Respond ONLY with valid JSON. Example:
{{"high_temp_threshold": 35, "low_moisture_threshold": 40}}"""

    try:
        response = chat(prompt)
        from .json_extract import extract_json
        result = extract_json(response)

        # Validate result has required keys
        if "high_temp_threshold" in result and "low_moisture_threshold" in result:
            return {
                "high_temp_threshold": float(result["high_temp_threshold"]),
                "low_moisture_threshold": float(result["low_moisture_threshold"]),
            }
    except Exception:
        pass

    return default
