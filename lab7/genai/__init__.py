"""GenAI utilities for Ollama integration."""

from genai.json_extract import extract_json_object
from genai.ollama_client import chat, generate

__all__ = ["extract_json_object", "chat", "generate"]
