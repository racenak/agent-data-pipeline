from __future__ import annotations

from memory.schema import Incident


def format_past_incidents(incidents: list[Incident]) -> str:
    if not incidents:
        return "No past incidents found for this error pattern."

    parts = ["PAST SIMILAR INCIDENTS (for reference — check if this is a recurring issue):\n"]

    for i, inc in enumerate(incidents, 1):
        ts = inc.created_at.strftime("%Y-%m-%d %H:%M") if inc.created_at else "unknown"
        status = "RESOLVED" if inc.resolved else "UNRESOLVED"
        parts.append(
            f"[{i}] {ts} | {inc.severity or '?'} | {inc.pipeline_name} | {status}\n"
            f"  Error Signature: {inc.error_signature}\n"
            f"  Summary: {inc.error_summary or 'N/A'}\n"
            f"  Root Cause: {inc.root_cause or 'N/A'}\n"
            f"  Resolution: {inc.resolution_notes or 'N/A'}"
        )

    return "\n\n".join(parts)
