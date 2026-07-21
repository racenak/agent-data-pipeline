from __future__ import annotations

import re


def extract_error_signature(raw_logs: str) -> str | None:
    """Extract a normalized error pattern from logs.

    Finds lines containing 'error' or 'exception' and normalizes them
    by stripping timestamps, PIDs, hex addresses, and variable values
    to create a reusable signature for matching similar incidents.

    Returns None if no error lines are found.
    """
    lines = raw_logs.split("\n")
    error_lines = [
        line for line in lines
        if "error" in line.lower() or "exception" in line.lower()
    ]

    if not error_lines:
        return None

    primary = error_lines[0]

    primary = re.sub(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[\.\d]*", "<TIMESTAMP>", primary)
    primary = re.sub(r"\b\d{5,}\b", "<ID>", primary)
    primary = re.sub(r"0x[0-9a-fA-F]+", "<ADDR>", primary)
    primary = re.sub(r"\bpid[=:\s]+\d+\b", "pid=<PID>", primary, flags=re.IGNORECASE)
    primary = re.sub(r"\s+", " ", primary).strip()

    return primary.lower()
