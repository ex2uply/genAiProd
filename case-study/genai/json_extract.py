"""Extract and validate JSON from LLM outputs."""
import json
import re


def extract_json(text: str) -> dict:
    """Extract JSON object from text that may contain markdown or extra text.

    Args:
        text: Raw text potentially containing JSON

    Returns:
        Parsed dictionary
    """
    # Try to find JSON in markdown code blocks
    if "```json" in text:
        match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            return json.loads(match.group(1))

    # Try to find any JSON object
    match = re.search(r"\{.*?\}", text, re.DOTALL)
    if match:
        return json.loads(match.group(0))

    # Try parsing the whole text
    return json.loads(text)
