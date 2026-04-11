"""Backward-compatible wrapper for Apple HR parser."""

from velomate.tools.merger.apple_parser import parse_apple_csv, parse_apple_hr, parse_apple_json

__all__ = ["parse_apple_hr", "parse_apple_json", "parse_apple_csv"]
