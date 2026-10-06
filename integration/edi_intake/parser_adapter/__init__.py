"""Offline attachment normalization, with no HTTP routes or persistence."""

from .adapter import AdapterError, parse_attachment, parse_base64_attachment

__all__ = ["AdapterError", "parse_attachment", "parse_base64_attachment"]
