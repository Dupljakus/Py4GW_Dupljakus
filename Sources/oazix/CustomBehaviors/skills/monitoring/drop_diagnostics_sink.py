from __future__ import annotations

import datetime
import os
from typing import Any

from Sources.oazix.CustomBehaviors.primitives import constants
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_event_emitter import append_event_payload_jsonl
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_event_schema import normalize_event_field_value

EXPECTED_RUNTIME_ERRORS = (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError, OSError)


def get_diagnostics_log_path(drop_log_path: str = "", file_name: str = "drop_tracker_live_debug.jsonl") -> str:
    base_path = str(drop_log_path or constants.DROP_LOG_PATH or "").strip()
    if not base_path:
        base_path = "Py4GW/drop_log.csv"
    return os.path.join(os.path.dirname(base_path), str(file_name or "drop_tracker_live_debug.jsonl").strip())


def append_diagnostics_event_jsonl(
    *,
    actor: str,
    event: str,
    message: str,
    drop_log_path: str = "",
    log_path: str = "",
    ts: str = "",
    **fields: Any,
) -> str:
    try:
        target_path = str(log_path or get_diagnostics_log_path(drop_log_path)).strip()
        if not target_path:
            return ""
        event_ts = str(ts or "").strip() or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        normalized_fields = {
            str(key or "").strip(): normalize_event_field_value(value)
            for key, value in fields.items()
            if str(key or "").strip()
        }
        return append_event_payload_jsonl(
            path=target_path,
            ts=event_ts,
            actor=str(actor or "").strip() or "unknown",
            event=str(event or "").strip() or "log",
            message=str(message or "").strip(),
            **normalized_fields,
        )
    except EXPECTED_RUNTIME_ERRORS:
        return ""
