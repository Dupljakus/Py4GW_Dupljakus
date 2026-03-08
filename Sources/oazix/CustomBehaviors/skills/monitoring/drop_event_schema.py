from __future__ import annotations

from typing import Any

EXPECTED_RUNTIME_ERRORS = (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError, OSError)
EVENT_BASE_FIELDS = ("ts", "actor", "event", "message")


def normalize_event_field_value(value: Any):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, tuple):
        return [normalize_event_field_value(part) for part in value]
    if isinstance(value, list):
        return [normalize_event_field_value(part) for part in value]
    if isinstance(value, dict):
        normalized = {}
        for key, item in value.items():
            normalized[str(key)] = normalize_event_field_value(item)
        return normalized
    return str(value)


def build_event_payload(
    *,
    ts: str,
    actor: str,
    event: str,
    message: str,
    **fields: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ts": str(ts or "").strip(),
        "actor": str(actor or "").strip() or "unknown",
        "event": str(event or "").strip() or "log",
        "message": str(message or "").strip(),
    }
    for key, value in fields.items():
        key_txt = str(key or "").strip()
        if not key_txt or key_txt in EVENT_BASE_FIELDS:
            continue
        payload[key_txt] = normalize_event_field_value(value)
    return payload


def normalize_parsed_event_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {
            "ts": "",
            "actor": "raw",
            "event": "raw",
            "message": str(payload or "").strip(),
        }
    normalized: dict[str, Any] = {
        "ts": str(payload.get("ts", "") or "").strip(),
        "actor": str(payload.get("actor", "") or "").strip() or "unknown",
        "event": str(payload.get("event", "") or "").strip() or "log",
        "message": str(payload.get("message", "") or "").strip(),
    }
    for key, value in payload.items():
        key_txt = str(key or "").strip()
        if not key_txt or key_txt in EVENT_BASE_FIELDS:
            continue
        normalized[key_txt] = normalize_event_field_value(value)
    return normalized
