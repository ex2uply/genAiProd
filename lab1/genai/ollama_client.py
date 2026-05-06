"""Minimal Ollama HTTP client (stdlib only).

Uses Ollama's `/api/chat` endpoint. Docs: https://github.com/ollama/ollama/blob/main/docs/api.md

Environment variables:
- OLLAMA_HOST: base URL, default http://127.0.0.1:11434
- OLLAMA_MODEL: model tag, default llama3
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


def _base_url() -> str:
    return os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")


def chat(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    timeout_s: float = 120.0,
) -> str:
    """
    Send a chat completion request to Ollama.

    `messages` format: [{"role": "user"|"system"|"assistant", "content": "..."}, ...]
    Returns assistant message content (non-streaming).
    """
    tag = model or os.environ.get("OLLAMA_MODEL", "llama3")
    url = f"{_base_url()}/api/chat"
    payload = {"model": tag, "messages": messages, "stream": False}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Ollama HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Could not reach Ollama at {_base_url()}. Is `ollama serve` running? ({e})"
        ) from e

    parsed = json.loads(raw)
    msg = parsed.get("message") or {}
    content = msg.get("content")
    if not isinstance(content, str):
        raise RuntimeError(f"Unexpected Ollama response shape: {parsed!r}")
    return content


def generate(prompt: str, *, model: str | None = None, system: str | None = None, timeout_s: float = 120.0) -> str:
    """Convenience wrapper: single user prompt (optional system message)."""
    messages: list[dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    return chat(messages, model=model, timeout_s=timeout_s)
