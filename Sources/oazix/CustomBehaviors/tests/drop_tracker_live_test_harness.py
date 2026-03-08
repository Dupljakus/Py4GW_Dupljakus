from __future__ import annotations

import csv
import datetime
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from Sources.oazix.CustomBehaviors.tests import drop_test_event_reader as event_reader


PY4GW_ROOT = event_reader.PY4GW_ROOT
WORK_ROOT = event_reader.WORK_ROOT
DATA_DIR = event_reader.DATA_DIR
DROP_LOG_PATH = event_reader.DROP_LOG_PATH
LIVE_DEBUG_PATH = event_reader.LIVE_DEBUG_PATH
RUNTIME_CONFIG_PATH = event_reader.RUNTIME_CONFIG_PATH
STATE_DIR = event_reader.STATE_DIR
STATE_PATH = event_reader.STATE_PATH
ORACLE_POLICY_PATH = event_reader.ORACLE_POLICY_PATH
BUNDLE_DIR = event_reader.BUNDLE_DIR
FORBIDDEN_ITEM_NAME_PATTERNS = (
    r"(?i)\b(?:expert|superior)?\s*(?:salvage|identification)\s+kit\b",
    r"(?i)\bid\s+kit\b",
)
FORBIDDEN_ITEM_NAME_REGEXES = tuple(re.compile(pattern) for pattern in FORBIDDEN_ITEM_NAME_PATTERNS)
FORBIDDEN_MODEL_IDS = frozenset({239, 2611, 2989, 2992, 5899})


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    return event_reader.load_csv_rows(path)


def _load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    return event_reader.load_jsonl_rows(path)


def _load_runtime_config() -> dict[str, Any]:
    return event_reader.load_runtime_config(RUNTIME_CONFIG_PATH)


def _write_state(state: dict[str, Any]) -> None:
    event_reader.write_state(state)


def _read_state() -> dict[str, Any]:
    return event_reader.read_state()


def _capture_current_state() -> dict[str, Any]:
    return event_reader.capture_current_state()


def _begin() -> int:
    return event_reader.begin()


def _refresh_baseline() -> dict[str, Any]:
    return event_reader.refresh_baseline()


def _row_item_label(row: dict[str, Any]) -> str:
    return event_reader.row_item_label(row)


def _parse_ts(value: Any) -> datetime.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return None


def _collect_likely_rezones(new_debug_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reset_rows = []
    for row in new_debug_rows:
        event_name = str(row.get("event", "")).strip()
        if event_name not in {"viewer_session_reset", "sender_session_reset"}:
            continue
        reason = str(row.get("reason", "")).strip().lower()
        uptime_ms = max(0, int(row.get("current_instance_uptime_ms", 0) or 0))
        if reason not in {"viewer_instance_reset", "viewer_sync_reset", "instance_change"} and uptime_ms > 5000:
            continue
        reset_rows.append(row)

    reset_rows.sort(
        key=lambda row: (
            _parse_ts(row.get("ts")) or datetime.datetime.min,
            max(0, int(row.get("current_map_id", 0) or 0)),
            max(0, int(row.get("current_instance_uptime_ms", 0) or 0)),
        )
    )

    rezones: list[dict[str, Any]] = []
    for row in reset_rows:
        ts_value = _parse_ts(row.get("ts"))
        map_id = max(0, int(row.get("current_map_id", 0) or 0))
        uptime_ms = max(0, int(row.get("current_instance_uptime_ms", 0) or 0))
        reason = str(row.get("reason", "")).strip() or "unknown"
        event_name = str(row.get("event", "")).strip() or "unknown"

        if rezones:
            last = rezones[-1]
            last_ts = _parse_ts(last.get("ts"))
            same_map = int(last.get("current_map_id", 0) or 0) == map_id
            close_in_time = bool(
                ts_value is not None
                and last_ts is not None
                and abs((ts_value - last_ts).total_seconds()) <= 8.0
            )
            if same_map and close_in_time:
                reasons = list(last.get("reasons", []) or [])
                if reason not in reasons:
                    reasons.append(reason)
                events = list(last.get("events", []) or [])
                if event_name not in events:
                    events.append(event_name)
                last["reasons"] = reasons
                last["events"] = events
                last["current_instance_uptime_ms"] = max(
                    max(0, int(last.get("current_instance_uptime_ms", 0) or 0)),
                    uptime_ms,
                )
                continue

        rezones.append(
            {
                "ts": str(row.get("ts", "") or "").strip(),
                "current_map_id": map_id,
                "current_instance_uptime_ms": uptime_ms,
                "reasons": [reason],
                "events": [event_name],
            }
        )
    return rezones


def _runtime_label_for_reset(row: dict[str, Any]) -> str:
    actor = str(row.get("actor", "") or "").strip().lower()
    if actor == "viewer":
        runtime_id = str(row.get("viewer_runtime_id", "") or "").strip()
        if runtime_id:
            return runtime_id
    if actor == "sender":
        runtime_id = str(row.get("sender_runtime_id", "") or "").strip()
        if runtime_id:
            return runtime_id
    fallback_reason = str(row.get("reason", "") or "").strip() or "unknown"
    fallback_map = max(0, int(row.get("current_map_id", 0) or 0))
    return f"{actor or 'unknown'}@map{fallback_map}:{fallback_reason}"


def _build_reset_runtime_breakdown(reset_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in list(reset_rows or []):
        if not isinstance(row, dict):
            continue
        actor = str(row.get("actor", "") or "").strip().lower() or "unknown"
        runtime_label = _runtime_label_for_reset(row)
        key = (actor, runtime_label)
        bucket = grouped.get(key)
        if bucket is None:
            bucket = {
                "actor": actor,
                "runtime_id": runtime_label,
                "count": 0,
                "latest_ts": "",
                "latest_reason": "",
                "latest_caller": "",
                "latest_source_runtime_id": "",
                "latest_source_caller": "",
                "latest_source_sequence": 0,
            }
            grouped[key] = bucket
        bucket["count"] = int(bucket.get("count", 0) or 0) + 1
        ts_value = str(row.get("ts", "") or "").strip()
        latest_ts = str(bucket.get("latest_ts", "") or "").strip()
        if ts_value >= latest_ts:
            bucket["latest_ts"] = ts_value
            bucket["latest_reason"] = str(row.get("reason", "") or "").strip()
            bucket["latest_caller"] = str(
                row.get("update_caller", "") or row.get("reset_source_caller", "") or ""
            ).strip()
            bucket["latest_source_runtime_id"] = str(row.get("reset_source_runtime_id", "") or "").strip()
            bucket["latest_source_caller"] = str(row.get("reset_source_caller", "") or "").strip()
            bucket["latest_source_sequence"] = max(0, _safe_int(row.get("reset_source_sequence", 0), 0))
    return sorted(
        grouped.values(),
        key=lambda row: (
            -int(row.get("count", 0) or 0),
            str(row.get("actor", "") or ""),
            str(row.get("runtime_id", "") or ""),
        ),
    )


def _latest_viewer_reset_ts(new_debug_rows: list[dict[str, Any]]) -> datetime.datetime | None:
    latest: datetime.datetime | None = None
    for row in new_debug_rows:
        if str(row.get("event", "")).strip() != "viewer_session_reset":
            continue
        ts_value = _parse_ts(row.get("ts"))
        if ts_value is None:
            continue
        if latest is None or ts_value > latest:
            latest = ts_value
    return latest


def _latest_sender_reset_ts(new_debug_rows: list[dict[str, Any]]) -> datetime.datetime | None:
    latest: datetime.datetime | None = None
    for row in new_debug_rows:
        if str(row.get("event", "")).strip() != "sender_session_reset":
            continue
        ts_value = _parse_ts(row.get("ts"))
        if ts_value is None:
            continue
        if latest is None or ts_value > latest:
            latest = ts_value
    return latest


def _latest_event_ts(new_debug_rows: list[dict[str, Any]], event_name: str) -> datetime.datetime | None:
    latest: datetime.datetime | None = None
    expected_event = str(event_name or "").strip()
    if not expected_event:
        return None
    for row in new_debug_rows:
        if str(row.get("event", "")).strip() != expected_event:
            continue
        ts_value = _parse_ts(row.get("ts"))
        if ts_value is None:
            continue
        if latest is None or ts_value > latest:
            latest = ts_value
    return latest


def _chat_pickup_label(row: dict[str, Any]) -> str:
    player_name = str(row.get("player_name", "") or "").strip() or "Unknown"
    item_name = str(row.get("item_name", "") or "").strip() or "Unknown Item"
    quantity = max(1, _safe_int(row.get("quantity", 1), 1))
    rarity = str(row.get("rarity", "") or "Unknown").strip() or "Unknown"
    return f"{item_name} x{quantity} ({rarity}) [{player_name}]"


def _slice_drop_rows_since_baseline(state: dict[str, Any], drop_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return event_reader.slice_drop_rows_since_baseline(state, drop_rows)


def _slice_debug_rows_since_baseline(state: dict[str, Any], debug_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return event_reader.slice_debug_rows_since_baseline(state, debug_rows)


def _slice_rows_since_baseline(
    state: dict[str, Any],
    drop_rows: list[dict[str, Any]],
    debug_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return event_reader.slice_rows_since_baseline(state, drop_rows, debug_rows)


def _clean_sender_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _event_id_from_csv_row(row: dict[str, Any]) -> str:
    return str(row.get("EventID", "") or "").strip()


def _event_id_from_debug_row(row: dict[str, Any]) -> str:
    return str(row.get("event_id", "") or "").strip()


def _csv_item_stats_text(row: dict[str, Any]) -> str:
    return str(row.get("ItemStats", "") or "").strip()


def _csv_sender_email(row: dict[str, Any]) -> str:
    return _clean_sender_email(row.get("SenderEmail", ""))


def _debug_sender_email(row: dict[str, Any]) -> str:
    return _clean_sender_email(row.get("sender_email", ""))


def _debug_receiver_email(row: dict[str, Any]) -> str:
    return _clean_sender_email(row.get("receiver_email", ""))


def _is_stats_exempt_rarity(rarity_value: Any) -> bool:
    rarity = str(rarity_value or "").strip().lower()
    return rarity in {"material", "dyes", "keys", "gold"}


def _is_forbidden_loot_row(row: dict[str, Any]) -> bool:
    name = str(row.get("ItemName", "") or row.get("item_name", "") or "").strip()
    model_id = max(0, _safe_int(row.get("ModelID", row.get("model_id", 0)) or 0, 0))
    if model_id in FORBIDDEN_MODEL_IDS:
        return True
    if not name:
        return False
    return any(regex.search(name) for regex in FORBIDDEN_ITEM_NAME_REGEXES)


def _build_event_lifecycle(
    new_drop_rows: list[dict[str, Any]],
    new_debug_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    lifecycle: dict[str, dict[str, Any]] = {}

    def _entry(event_id: str) -> dict[str, Any]:
        if event_id not in lifecycle:
            lifecycle[event_id] = {
                "event_id": event_id,
                "sender_email": "",
                "receiver_email": "",
                "receiver_in_party": None,
                "role": "",
                "item_name": "",
                "label": event_id,
                "target_resolved": False,
                "invalid_target": False,
                "accepted": False,
                "sent": False,
                "acked": False,
                "csv": False,
                "stats_bound": False,
                "stats_name_mismatch": False,
                "send_failed": False,
                "send_failed_unresolved": False,
                "send_failed_recovered": False,
                "csv_has_stats": False,
                "csv_rarity": "Unknown",
                "csv_label": "",
                "csv_rows": 0,
                "debug_events": [],
            }
        return lifecycle[event_id]

    for row in list(new_debug_rows or []):
        event_id = _event_id_from_debug_row(row)
        if not event_id:
            continue
        event_name = str(row.get("event", "") or "").strip()
        entry = _entry(event_id)
        if event_name and event_name not in entry["debug_events"]:
            entry["debug_events"].append(event_name)
        sender_email = _debug_sender_email(row)
        if sender_email and not entry["sender_email"]:
            entry["sender_email"] = sender_email
        receiver_email = _debug_receiver_email(row)
        if receiver_email and not entry["receiver_email"]:
            entry["receiver_email"] = receiver_email
        role = str(row.get("role", "") or "").strip()
        if role and not entry["role"]:
            entry["role"] = role
        item_name = str(row.get("item_name", "") or "").strip()
        if item_name and not entry["item_name"]:
            entry["item_name"] = item_name
            entry["label"] = item_name
        if event_name == "viewer_drop_accepted":
            entry["accepted"] = True
        elif event_name == "tracker_transport_target_resolved":
            entry["target_resolved"] = True
            in_party_flag = row.get("receiver_in_party", None)
            if isinstance(in_party_flag, bool):
                entry["receiver_in_party"] = in_party_flag
                if not in_party_flag:
                    entry["invalid_target"] = True
        elif event_name == "tracker_drop_sent":
            entry["sent"] = True
        elif event_name == "tracker_drop_acked":
            entry["acked"] = True
        elif event_name == "tracker_drop_send_failed":
            entry["send_failed"] = True
        elif event_name in {"viewer_stats_payload_bound", "viewer_stats_text_bound"}:
            entry["stats_bound"] = True
        elif event_name == "viewer_stats_name_mismatch":
            entry["stats_name_mismatch"] = True

    for row in list(new_drop_rows or []):
        event_id = _event_id_from_csv_row(row)
        if not event_id:
            continue
        entry = _entry(event_id)
        entry["csv"] = True
        entry["csv_rows"] = int(entry.get("csv_rows", 0) or 0) + 1
        stats_text = _csv_item_stats_text(row)
        if stats_text:
            entry["csv_has_stats"] = True
        sender_email = _csv_sender_email(row)
        if sender_email and not entry["sender_email"]:
            entry["sender_email"] = sender_email
        entry["csv_rarity"] = str(row.get("Rarity", "") or "Unknown").strip() or "Unknown"
        entry["csv_label"] = _row_item_label(row)
        csv_item_name = str(row.get("ItemName", "") or "").strip()
        if csv_item_name and not entry["item_name"]:
            entry["item_name"] = csv_item_name
        entry["label"] = entry["csv_label"] or entry["item_name"] or event_id

    lifecycle_rows = sorted(
        lifecycle.values(),
        key=lambda row: (
            str(row.get("sender_email", "") or ""),
            str(row.get("receiver_email", "") or ""),
            str(row.get("event_id", "") or ""),
        ),
    )

    lifecycle_gaps: list[dict[str, Any]] = []
    accepted_missing_stats_binding: list[dict[str, Any]] = []
    for row in lifecycle_rows:
        event_id = str(row.get("event_id", "") or "").strip()
        if not event_id:
            continue
        sender_email = str(row.get("sender_email", "") or "").strip().lower()
        receiver_email = str(row.get("receiver_email", "") or "").strip().lower()
        label = (
            str(row.get("csv_label", "") or "").strip()
            or str(row.get("item_name", "") or "").strip()
            or event_id
        )
        gap_codes: list[str] = []
        owner_hints: list[str] = []

        def _append_gap(code: str, severity: str, owner_hint: str) -> None:
            lifecycle_gaps.append(
                {
                    "severity": severity,
                    "code": code,
                    "owner_hint": owner_hint,
                    "event_id": event_id,
                    "sender_email": sender_email,
                    "receiver_email": receiver_email,
                    "label": label,
                }
            )
            if code not in gap_codes:
                gap_codes.append(code)
                owner_hints.append(owner_hint)

        send_failed_flag = bool(row.get("send_failed", False))
        send_failed_unresolved = bool(
            send_failed_flag
            and not bool(row.get("sent", False))
            and not bool(row.get("accepted", False))
            and not bool(row.get("acked", False))
            and not bool(row.get("csv", False))
        )
        row["send_failed_unresolved"] = send_failed_unresolved
        row["send_failed_recovered"] = bool(send_failed_flag and not send_failed_unresolved)

        if bool(row.get("invalid_target", False)):
            _append_gap("invalid_target", "critical", "sender")
        if send_failed_unresolved:
            _append_gap("send_failed", "critical", "sender")
        if (
            bool(row.get("target_resolved", False))
            and not bool(row.get("sent", False))
            and not send_failed_unresolved
            and not bool(row.get("accepted", False))
        ):
            _append_gap("resolved_missing_send", "major", "sender")
        if bool(row.get("sent", False)) and not bool(row.get("accepted", False)):
            _append_gap("sent_missing_accepted", "critical", "receiver")
        if bool(row.get("accepted", False)) and not bool(row.get("acked", False)):
            _append_gap("accepted_missing_ack", "major", "receiver_ack")
        if bool(row.get("accepted", False)) and not bool(row.get("csv", False)):
            _append_gap("accepted_missing_csv", "critical", "viewer_csv")
        if bool(row.get("csv", False)) and not bool(row.get("accepted", False)):
            _append_gap("csv_missing_accepted", "critical", "receiver")
        if (
            bool(row.get("accepted", False))
            and bool(row.get("csv", False))
            and (not bool(row.get("stats_bound", False)))
            and (not bool(row.get("csv_has_stats", False)))
            and (not _is_stats_exempt_rarity(row.get("csv_rarity", "Unknown")))
        ):
            accepted_missing_stats_binding.append(
                {
                    "event_id": event_id,
                    "sender_email": sender_email,
                    "receiver_email": receiver_email,
                    "label": label,
                    "rarity": str(row.get("csv_rarity", "") or "Unknown").strip() or "Unknown",
                }
            )
            _append_gap("accepted_missing_stats_binding", "major", "viewer_stats")
        row["gap_codes"] = gap_codes
        row["owner_hints"] = owner_hints
        row["primary_gap_code"] = gap_codes[0] if gap_codes else ""
        row["primary_owner_hint"] = owner_hints[0] if owner_hints else ""
        row["label"] = label
        row["problem_event"] = bool(gap_codes)
    return lifecycle_rows, lifecycle_gaps, accepted_missing_stats_binding


def _build_sender_lifecycle_summary(lifecycle_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_sender: dict[str, dict[str, Any]] = {}
    for row in list(lifecycle_rows or []):
        sender_email = str(row.get("sender_email", "") or "").strip().lower() or "unknown"
        sender_entry = by_sender.get(sender_email)
        if sender_entry is None:
            sender_entry = {
                "sender_email": sender_email,
                "events": 0,
                "accepted": 0,
                "sent": 0,
                "acked": 0,
                "csv": 0,
                "stats_bound": 0,
                "send_failed": 0,
                "accepted_missing_csv": 0,
                "csv_missing_accepted": 0,
                "accepted_missing_stats_binding": 0,
                "problem_events": 0,
            }
            by_sender[sender_email] = sender_entry
        sender_entry["events"] = int(sender_entry.get("events", 0)) + 1
        if bool(row.get("accepted", False)):
            sender_entry["accepted"] = int(sender_entry.get("accepted", 0)) + 1
        if bool(row.get("sent", False)):
            sender_entry["sent"] = int(sender_entry.get("sent", 0)) + 1
        if bool(row.get("acked", False)):
            sender_entry["acked"] = int(sender_entry.get("acked", 0)) + 1
        if bool(row.get("csv", False)):
            sender_entry["csv"] = int(sender_entry.get("csv", 0)) + 1
        if bool(row.get("stats_bound", False)):
            sender_entry["stats_bound"] = int(sender_entry.get("stats_bound", 0)) + 1
        if bool(row.get("send_failed_unresolved", False)):
            sender_entry["send_failed"] = int(sender_entry.get("send_failed", 0)) + 1
        if bool(row.get("problem_event", False)):
            sender_entry["problem_events"] = int(sender_entry.get("problem_events", 0)) + 1
        if bool(row.get("accepted", False)) and (not bool(row.get("csv", False))):
            sender_entry["accepted_missing_csv"] = int(sender_entry.get("accepted_missing_csv", 0)) + 1
        if bool(row.get("csv", False)) and (not bool(row.get("accepted", False))):
            sender_entry["csv_missing_accepted"] = int(sender_entry.get("csv_missing_accepted", 0)) + 1
        if (
            bool(row.get("accepted", False))
            and bool(row.get("csv", False))
            and (not bool(row.get("stats_bound", False)))
            and (not bool(row.get("csv_has_stats", False)))
            and (not _is_stats_exempt_rarity(row.get("csv_rarity", "Unknown")))
        ):
            sender_entry["accepted_missing_stats_binding"] = int(
                sender_entry.get("accepted_missing_stats_binding", 0)
            ) + 1

    return sorted(
        by_sender.values(),
        key=lambda row: (
            -int(row.get("accepted_missing_csv", 0) or 0),
            -int(row.get("csv_missing_accepted", 0) or 0),
            -int(row.get("accepted_missing_stats_binding", 0) or 0),
            str(row.get("sender_email", "") or ""),
        ),
    )


def _has_meaningful_route_signal(row: dict[str, Any]) -> bool:
    return bool(
        row.get("target_resolved", False)
        or row.get("invalid_target", False)
        or row.get("sent", False)
        or row.get("accepted", False)
        or row.get("acked", False)
        or row.get("csv", False)
        or row.get("problem_event", False)
    )


def _build_receiver_lifecycle_summary(lifecycle_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_receiver: dict[str, dict[str, Any]] = {}
    for row in list(lifecycle_rows or []):
        receiver_email = str(row.get("receiver_email", "") or "").strip().lower() or "unknown"
        if receiver_email == "unknown" and not _has_meaningful_route_signal(row):
            continue
        receiver_entry = by_receiver.get(receiver_email)
        if receiver_entry is None:
            receiver_entry = {
                "receiver_email": receiver_email,
                "events": 0,
                "target_resolved": 0,
                "accepted": 0,
                "acked": 0,
                "csv": 0,
                "invalid_target": 0,
                "sent_missing_accepted": 0,
                "accepted_missing_ack": 0,
                "accepted_missing_csv": 0,
                "problem_events": 0,
            }
            by_receiver[receiver_email] = receiver_entry
        receiver_entry["events"] = int(receiver_entry.get("events", 0)) + 1
        if bool(row.get("target_resolved", False)):
            receiver_entry["target_resolved"] = int(receiver_entry.get("target_resolved", 0)) + 1
        if bool(row.get("accepted", False)):
            receiver_entry["accepted"] = int(receiver_entry.get("accepted", 0)) + 1
        if bool(row.get("acked", False)):
            receiver_entry["acked"] = int(receiver_entry.get("acked", 0)) + 1
        if bool(row.get("csv", False)):
            receiver_entry["csv"] = int(receiver_entry.get("csv", 0)) + 1
        if bool(row.get("invalid_target", False)):
            receiver_entry["invalid_target"] = int(receiver_entry.get("invalid_target", 0)) + 1
        if bool(row.get("sent", False)) and (not bool(row.get("accepted", False))):
            receiver_entry["sent_missing_accepted"] = int(receiver_entry.get("sent_missing_accepted", 0)) + 1
        if bool(row.get("accepted", False)) and (not bool(row.get("acked", False))):
            receiver_entry["accepted_missing_ack"] = int(receiver_entry.get("accepted_missing_ack", 0)) + 1
        if bool(row.get("accepted", False)) and (not bool(row.get("csv", False))):
            receiver_entry["accepted_missing_csv"] = int(receiver_entry.get("accepted_missing_csv", 0)) + 1
        if bool(row.get("problem_event", False)):
            receiver_entry["problem_events"] = int(receiver_entry.get("problem_events", 0)) + 1

    return sorted(
        by_receiver.values(),
        key=lambda row: (
            -int(row.get("problem_events", 0) or 0),
            -int(row.get("sent_missing_accepted", 0) or 0),
            -int(row.get("accepted_missing_csv", 0) or 0),
            str(row.get("receiver_email", "") or ""),
        ),
    )


def _build_route_lifecycle_summary(lifecycle_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_route: dict[tuple[str, str], dict[str, Any]] = {}
    for row in list(lifecycle_rows or []):
        sender_email = str(row.get("sender_email", "") or "").strip().lower() or "unknown"
        receiver_email = str(row.get("receiver_email", "") or "").strip().lower() or "unknown"
        if receiver_email == "unknown" and not _has_meaningful_route_signal(row):
            continue
        route_key = (sender_email, receiver_email)
        route_entry = by_route.get(route_key)
        if route_entry is None:
            route_entry = {
                "sender_email": sender_email,
                "receiver_email": receiver_email,
                "events": 0,
                "target_resolved": 0,
                "sent": 0,
                "accepted": 0,
                "acked": 0,
                "csv": 0,
                "invalid_target": 0,
                "send_failed": 0,
                "problem_events": 0,
            }
            by_route[route_key] = route_entry
        route_entry["events"] = int(route_entry.get("events", 0)) + 1
        if bool(row.get("target_resolved", False)):
            route_entry["target_resolved"] = int(route_entry.get("target_resolved", 0)) + 1
        if bool(row.get("sent", False)):
            route_entry["sent"] = int(route_entry.get("sent", 0)) + 1
        if bool(row.get("accepted", False)):
            route_entry["accepted"] = int(route_entry.get("accepted", 0)) + 1
        if bool(row.get("acked", False)):
            route_entry["acked"] = int(route_entry.get("acked", 0)) + 1
        if bool(row.get("csv", False)):
            route_entry["csv"] = int(route_entry.get("csv", 0)) + 1
        if bool(row.get("invalid_target", False)):
            route_entry["invalid_target"] = int(route_entry.get("invalid_target", 0)) + 1
        if bool(row.get("send_failed_unresolved", False)):
            route_entry["send_failed"] = int(route_entry.get("send_failed", 0)) + 1
        if bool(row.get("problem_event", False)):
            route_entry["problem_events"] = int(route_entry.get("problem_events", 0)) + 1

    return sorted(
        by_route.values(),
        key=lambda row: (
            -int(row.get("problem_events", 0) or 0),
            -int(row.get("events", 0) or 0),
            str(row.get("sender_email", "") or ""),
            str(row.get("receiver_email", "") or ""),
        ),
    )


def _build_noise_cost_summary(new_debug_rows: list[dict[str, Any]]) -> dict[str, Any]:
    viewer_reset_perf_rows = [
        row for row in list(new_debug_rows or []) if str(row.get("event", "") or "").strip() == "viewer_reset_perf"
    ]
    sender_inventory_perf_rows = [
        row for row in list(new_debug_rows or []) if str(row.get("event", "") or "").strip() == "sender_inventory_perf"
    ]

    viewer_durations = [
        _safe_float(row.get("duration_ms", 0.0), 0.0)
        for row in viewer_reset_perf_rows
        if _safe_float(row.get("duration_ms", 0.0), 0.0) > 0.0
    ]
    sender_durations = [
        _safe_float(row.get("process_duration_ms", 0.0), 0.0)
        for row in sender_inventory_perf_rows
        if _safe_float(row.get("process_duration_ms", 0.0), 0.0) > 0.0
    ]

    viewer_avg = (sum(viewer_durations) / len(viewer_durations)) if viewer_durations else 0.0
    sender_avg = (sum(sender_durations) / len(sender_durations)) if sender_durations else 0.0
    viewer_max = max(viewer_durations or [0.0])
    sender_max = max(sender_durations or [0.0])
    max_duration = max(viewer_max, sender_max)
    avg_duration = max(viewer_avg, sender_avg)

    if not viewer_durations and not sender_durations:
        assessment = "not_measured"
    elif max_duration >= 60.0 or avg_duration >= 25.0:
        assessment = "potentially_material"
    elif max_duration >= 25.0 or avg_duration >= 10.0:
        assessment = "minor_overhead"
    else:
        assessment = "mostly_messy"

    return {
        "assessment": assessment,
        "viewer_reset_perf_count": len(viewer_durations),
        "viewer_reset_perf_avg_ms": round(viewer_avg, 2),
        "viewer_reset_perf_max_ms": round(viewer_max, 2),
        "sender_noise_perf_count": len(sender_durations),
        "sender_noise_perf_avg_ms": round(sender_avg, 2),
        "sender_noise_perf_max_ms": round(sender_max, 2),
    }


def _summarize(new_drop_rows: list[dict[str, Any]], new_debug_rows: list[dict[str, Any]]) -> dict[str, Any]:
    runtime_config = _load_runtime_config()
    chat_item_tracking_enabled = bool(runtime_config.get("enable_chat_item_tracking", False))
    accepted = [row for row in new_debug_rows if str(row.get("event", "")) == "viewer_drop_accepted"]
    duplicates = [row for row in new_debug_rows if str(row.get("event", "")) == "viewer_drop_duplicate"]
    suppressed = [row for row in new_debug_rows if str(row.get("event", "")).startswith("candidate_suppressed_")]
    sent = [row for row in new_debug_rows if str(row.get("event", "")) == "tracker_drop_sent"]
    send_failed = [row for row in new_debug_rows if str(row.get("event", "")) == "tracker_drop_send_failed"]
    acked = [row for row in new_debug_rows if str(row.get("event", "")) == "tracker_drop_acked"]
    resets = [row for row in new_debug_rows if str(row.get("event", "")).endswith("session_reset")]
    row_name_updates = [row for row in new_debug_rows if str(row.get("event", "")) == "viewer_row_name_updated"]
    stats_name_mismatches = [
        row
        for row in new_debug_rows
        if str(row.get("event", "")) in {"viewer_stats_name_mismatch", "viewer_selected_stats_name_mismatch"}
    ]
    target_resolved = [row for row in new_debug_rows if str(row.get("event", "")) == "tracker_transport_target_resolved"]
    chat_pickups = [row for row in new_debug_rows if str(row.get("event", "")) == "viewer_chat_pickup_observed"]
    startup_cycles = [row for row in new_debug_rows if str(row.get("event", "")) == "sender_startup_cycle"]
    startup_completed = [row for row in new_debug_rows if str(row.get("event", "")) == "sender_startup_completed"]
    post_reset_scans = [row for row in new_debug_rows if str(row.get("event", "")) == "sender_post_reset_scan_scheduled"]
    viewer_heartbeats = [row for row in new_debug_rows if str(row.get("event", "")) == "viewer_runtime_heartbeat"]
    sender_heartbeats = [row for row in new_debug_rows if str(row.get("event", "")) == "sender_runtime_heartbeat"]
    rezones = _collect_likely_rezones(new_debug_rows)
    reset_runtime_breakdown = _build_reset_runtime_breakdown(resets)
    noise_cost_summary = _build_noise_cost_summary(new_debug_rows)
    latest_reset_ts = _latest_viewer_reset_ts(new_debug_rows)
    latest_sender_reset_ts = _latest_sender_reset_ts(new_debug_rows)
    latest_viewer_heartbeat_ts = _latest_event_ts(new_debug_rows, "viewer_runtime_heartbeat")
    latest_sender_heartbeat_ts = _latest_event_ts(new_debug_rows, "sender_runtime_heartbeat")

    accepted_by_event = {
        str(row.get("event_id", "")).strip(): row
        for row in accepted
        if str(row.get("event_id", "")).strip()
    }
    csv_by_event = {
        str(row.get("EventID", "")).strip(): row
        for row in new_drop_rows
        if str(row.get("EventID", "")).strip()
    }

    missing_in_csv = [
        _row_item_label(row)
        for event_id, row in accepted_by_event.items()
        if event_id not in csv_by_event
    ]
    missing_in_accepted = [
        _row_item_label(row)
        for event_id, row in csv_by_event.items()
        if event_id not in accepted_by_event
    ]
    tracked_labels = {
        _row_item_label(row)
        for row in list(new_drop_rows or [])
        if isinstance(row, dict)
    }
    tracked_labels.update(_row_item_label(row) for row in accepted)
    uncorrelated_chat_pickups = [
        _chat_pickup_label(row)
        for row in chat_pickups
        if _chat_pickup_label(row) not in tracked_labels
    ]

    accepted_latest_session = []
    if latest_reset_ts is None:
        accepted_latest_session = list(accepted)
    else:
        for row in accepted:
            row_ts = _parse_ts(row.get("ts"))
            if row_ts is None or row_ts >= latest_reset_ts:
                accepted_latest_session.append(row)

    sender_rows_after_latest_reset = []
    if latest_sender_reset_ts is not None:
        for row in list(new_debug_rows or []):
            if str(row.get("actor", "") or "").strip() != "sender":
                continue
            row_ts = _parse_ts(row.get("ts"))
            if row_ts is None or row_ts < latest_sender_reset_ts:
                continue
            sender_rows_after_latest_reset.append(row)

    accepted_by_event_latest_session = {
        str(row.get("event_id", "")).strip(): row
        for row in accepted_latest_session
        if str(row.get("event_id", "")).strip()
    }
    latest_session_missing_in_csv = [
        _row_item_label(row)
        for event_id, row in accepted_by_event_latest_session.items()
        if event_id not in csv_by_event
    ]

    row_counter = Counter(_row_item_label(row) for row in new_drop_rows)
    duplicate_row_labels = [label for label, count in row_counter.items() if count > 1]
    csv_event_id_counter = Counter(
        str(row.get("EventID", "")).strip()
        for row in new_drop_rows
        if str(row.get("EventID", "")).strip()
    )
    duplicate_csv_event_ids = [
        event_id for event_id, count in csv_event_id_counter.items() if count > 1
    ]
    suspicious_name_updates = []
    for row in row_name_updates:
        previous_name = str(row.get("previous_name", "") or "").strip()
        new_name = str(row.get("new_name", "") or "").strip()
        previous_was_unknown = bool(row.get("previous_was_unknown", False))
        if not previous_name or not new_name or previous_name == new_name:
            continue
        if previous_was_unknown:
            continue
        if previous_name.lower() in new_name.lower() and len(new_name) > len(previous_name):
            continue
        suspicious_name_updates.append(
            {
                "event_id": str(row.get("event_id", "") or "").strip(),
                "player_name": str(row.get("player_name", "") or "").strip(),
                "sender_email": str(row.get("sender_email", "") or "").strip(),
                "rarity": str(row.get("rarity", "") or "").strip(),
                "previous_name": previous_name,
                "new_name": new_name,
                "update_source": str(row.get("update_source", "") or "").strip(),
            }
        )

    invalid_target_events = []
    for row in target_resolved:
        receiver_email = str(row.get("receiver_email", "") or "").strip().lower()
        if not receiver_email:
            continue
        in_party_flag = row.get("receiver_in_party", None)
        if isinstance(in_party_flag, bool):
            if not in_party_flag:
                invalid_target_events.append(
                    {
                        "event_id": str(row.get("event_id", "") or "").strip(),
                        "sender_email": str(row.get("sender_email", "") or "").strip().lower(),
                        "receiver_email": receiver_email,
                        "party_member_emails": list(row.get("party_member_emails", []) or [])[:24],
                    }
                )
            continue
        party_member_emails = [
            str(value or "").strip().lower()
            for value in list(row.get("party_member_emails", []) or [])
            if str(value or "").strip()
        ]
        if party_member_emails and receiver_email not in set(party_member_emails):
            invalid_target_events.append(
                {
                    "event_id": str(row.get("event_id", "") or "").strip(),
                    "sender_email": str(row.get("sender_email", "") or "").strip().lower(),
                    "receiver_email": receiver_email,
                    "party_member_emails": party_member_emails[:24],
                }
            )

    forbidden_rows = []
    for row in list(new_drop_rows or []):
        if not isinstance(row, dict):
            continue
        if not _is_forbidden_loot_row(row):
            continue
        forbidden_rows.append(
            {
                "event_id": _event_id_from_csv_row(row),
                "sender_email": _csv_sender_email(row),
                "item_name": str(row.get("ItemName", "") or row.get("item_name", "") or "").strip() or "Unknown Item",
                "rarity": str(row.get("Rarity", "") or row.get("rarity", "") or "Unknown").strip() or "Unknown",
                "model_id": max(0, _safe_int(row.get("ModelID", row.get("model_id", 0)) or 0, 0)),
                "label": _row_item_label(row),
            }
        )

    lifecycle_rows, lifecycle_gaps, accepted_missing_stats_binding = _build_event_lifecycle(
        new_drop_rows,
        new_debug_rows,
    )
    sender_lifecycle = _build_sender_lifecycle_summary(lifecycle_rows)
    receiver_lifecycle = _build_receiver_lifecycle_summary(lifecycle_rows)
    route_lifecycle = _build_route_lifecycle_summary(lifecycle_rows)
    problem_lifecycle_rows = [
        row for row in list(lifecycle_rows or []) if bool(row.get("problem_event", False))
    ]
    unresolved_send_failed_events = [
        row
        for row in list(problem_lifecycle_rows or [])
        if "send_failed" in list(row.get("gap_codes", []) or [])
    ]
    recovered_send_failed_events = [
        row
        for row in list(lifecycle_rows or [])
        if bool(row.get("send_failed_recovered", False))
    ]

    return {
        "new_drop_rows": len(new_drop_rows),
        "new_debug_rows": len(new_debug_rows),
        "accepted_count": len(accepted),
        "sent_count": len(sent),
        "send_failed_count": len(unresolved_send_failed_events),
        "send_failed_raw_count": len(send_failed),
        "send_failed_recovered_count": len(recovered_send_failed_events),
        "acked_count": len(acked),
        "duplicate_event_count": len(duplicates),
        "suppressed_event_count": len(suppressed),
        "reset_event_count": len(resets),
        "reset_runtime_count": len(reset_runtime_breakdown),
        "viewer_reset_event_count": len(
            [row for row in resets if str(row.get("event", "") or "").strip() == "viewer_session_reset"]
        ),
        "sender_reset_event_count": len(
            [row for row in resets if str(row.get("event", "") or "").strip() == "sender_session_reset"]
        ),
        "viewer_reset_runtime_count": len(
            {
                str(row.get("runtime_id", "") or "").strip()
                for row in reset_runtime_breakdown
                if str(row.get("actor", "") or "").strip() == "viewer"
                and str(row.get("runtime_id", "") or "").strip()
            }
        ),
        "sender_reset_runtime_count": len(
            {
                str(row.get("runtime_id", "") or "").strip()
                for row in reset_runtime_breakdown
                if str(row.get("actor", "") or "").strip() == "sender"
                and str(row.get("runtime_id", "") or "").strip()
            }
        ),
        "max_reset_events_per_runtime": max(
            [int(row.get("count", 0) or 0) for row in reset_runtime_breakdown] or [0]
        ),
        "rezone_count": len(rezones),
        "chat_item_tracking_enabled": chat_item_tracking_enabled,
        "chat_pickup_count": len(chat_pickups),
        "uncorrelated_chat_pickup_count": len(uncorrelated_chat_pickups),
        "viewer_heartbeat_count": len(viewer_heartbeats),
        "sender_heartbeat_count": len(sender_heartbeats),
        "sender_startup_cycle_count": len(startup_cycles),
        "sender_startup_completed_count": len(startup_completed),
        "sender_post_reset_scan_count": len(post_reset_scans),
        "sender_rows_after_latest_reset_count": len(sender_rows_after_latest_reset),
        "window_ended_during_sender_startup": bool(
            latest_sender_reset_ts is not None
            and len(startup_cycles) > 0
            and len(startup_completed) == 0
            and len(accepted) == 0
            and len(sent) == 0
        ),
        "window_ended_before_post_reset_sender_cycle": bool(
            latest_sender_reset_ts is not None
            and len(sender_rows_after_latest_reset) <= len(
                [
                    row
                    for row in sender_rows_after_latest_reset
                    if str(row.get("event", "") or "").strip() == "sender_session_reset"
                ]
            )
            and len(accepted) == 0
            and len(sent) == 0
        ),
        "runtime_log_silent_after_reset": bool(
            latest_reset_ts is not None
            and len(accepted) == 0
            and len(sent) == 0
            and len(chat_pickups) == 0
            and (
                latest_viewer_heartbeat_ts is None
                or latest_viewer_heartbeat_ts <= latest_reset_ts
            )
            and (
                latest_sender_reset_ts is None
                or latest_sender_heartbeat_ts is None
                or latest_sender_heartbeat_ts <= latest_sender_reset_ts
            )
        ),
        "row_name_update_count": len(row_name_updates),
        "suspicious_name_update_count": len(suspicious_name_updates),
        "stats_name_mismatch_count": len(stats_name_mismatches),
        "invalid_target_count": len(invalid_target_events),
        "forbidden_row_count": len(forbidden_rows),
        "lifecycle_event_count": len(lifecycle_rows),
        "lifecycle_gap_count": len(lifecycle_gaps),
        "accepted_missing_stats_binding_count": len(accepted_missing_stats_binding),
        "drop_rows": [_row_item_label(row) for row in new_drop_rows],
        "accepted_rows": [_row_item_label(row) for row in accepted],
        "missing_in_csv": missing_in_csv,
        "latest_session_missing_in_csv": latest_session_missing_in_csv,
        "latest_session_accepted_count": len(accepted_latest_session),
        "missing_in_accepted": missing_in_accepted,
        "chat_pickup_rows": [_chat_pickup_label(row) for row in chat_pickups[:24]],
        "uncorrelated_chat_pickups": uncorrelated_chat_pickups[:24],
        "viewer_heartbeats": viewer_heartbeats[:16],
        "sender_heartbeats": sender_heartbeats[:16],
        "duplicate_drop_rows": duplicate_row_labels,
        "duplicate_csv_event_ids": duplicate_csv_event_ids,
        "row_name_updates": row_name_updates[:20],
        "suspicious_name_updates": suspicious_name_updates[:20],
        "stats_name_mismatches": stats_name_mismatches[:20],
        "invalid_target_events": invalid_target_events[:20],
        "forbidden_rows": forbidden_rows[:24],
        "lifecycle_gaps": lifecycle_gaps[:60],
        "accepted_missing_stats_binding": accepted_missing_stats_binding[:40],
        "lifecycle_rows": lifecycle_rows[:180],
        "problem_lifecycle_rows": problem_lifecycle_rows[:80],
        "sender_lifecycle": sender_lifecycle[:40],
        "receiver_lifecycle": receiver_lifecycle[:40],
        "route_lifecycle": route_lifecycle[:60],
        "send_failed_events": unresolved_send_failed_events[:20],
        "send_failed_raw_events": send_failed[:20],
        "send_failed_recovered_events": recovered_send_failed_events[:20],
        "suppressed_events": suppressed[:12],
        "sender_startup_cycles": startup_cycles[:20],
        "sender_startup_completed": startup_completed[:20],
        "sender_post_reset_scans": post_reset_scans[:20],
        "noise_cost_summary": noise_cost_summary,
        "reset_runtime_breakdown": reset_runtime_breakdown[:16],
        "rezones": rezones[:12],
        "recent_resets": resets[-12:],
    }


def _end() -> int:
    state = _read_state()
    drop_rows = _load_csv_rows(DROP_LOG_PATH)
    debug_rows = _load_jsonl_rows(LIVE_DEBUG_PATH)

    new_drop_rows, new_debug_rows = _slice_rows_since_baseline(state, drop_rows, debug_rows)
    summary = _summarize(new_drop_rows, new_debug_rows)
    print(json.dumps(summary, indent=2))
    return 0


def _status() -> int:
    payload = {
        "drop_log_exists": DROP_LOG_PATH.exists(),
        "live_debug_exists": LIVE_DEBUG_PATH.exists(),
        "baseline_exists": STATE_PATH.exists(),
        "drop_row_count": len(_load_csv_rows(DROP_LOG_PATH)),
        "debug_row_count": len(_load_jsonl_rows(LIVE_DEBUG_PATH)),
    }
    print(json.dumps(payload, indent=2))
    return 0


def main(argv: list[str]) -> int:
    command = argv[1].strip().lower() if len(argv) > 1 else "status"
    if command == "begin":
        return _begin()
    if command == "end":
        return _end()
    if command == "status":
        return _status()
    raise SystemExit("Usage: python drop_tracker_live_test_harness.py [status|begin|end]")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
