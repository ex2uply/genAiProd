"""Extract a single JSON object from model output (handles extra prose / fences)."""

from __future__ import annotations

import json


def extract_json_object(text: str) -> dict:
    text = text.strip()
    try:
        out = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(f"No JSON object found in model output: {text[:500]!r}")
        out = json.loads(text[start : end + 1])
    if not isinstance(out, dict):
        raise TypeError(f"Expected JSON object, got {type(out)}")
    return out
