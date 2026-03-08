from __future__ import annotations

import json
import os
from typing import Any

from Sources.oazix.CustomBehaviors.skills.monitoring.drop_event_schema import build_event_payload

EXPECTED_RUNTIME_ERRORS = (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError, OSError)


def append_event_payload_jsonl(
    *,
    path: str,
    ts: str,
    actor: str,
    event: str,
    message: str,
    **fields: Any,
) -> str:
    try:
        target_path = str(path or "").strip()
        if not target_path:
            return ""
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        payload = build_event_payload(
            ts=ts,
            actor=actor,
            event=event,
            message=message,
            **fields,
        )
        with open(target_path, mode="a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
        return target_path
    except EXPECTED_RUNTIME_ERRORS:
        return ""
