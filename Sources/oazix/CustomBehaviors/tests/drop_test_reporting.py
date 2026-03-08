from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any, Callable


def default_oracle_policy() -> dict[str, Any]:
    return {
        "max_send_failed_count": 0,
        "max_missing_in_csv": 0,
        "max_latest_session_missing_in_csv": 0,
        "max_missing_in_accepted": 0,
        "max_duplicate_csv_event_ids": 0,
        "max_suspicious_name_update_count": 0,
        "max_stats_name_mismatch_count": 0,
        "max_invalid_target_count": 0,
        "max_forbidden_row_count": 0,
        "max_lifecycle_gap_count": 0,
        "max_accepted_missing_stats_binding_count": 0,
        "warn_duplicate_event_count_above": 0,
        "warn_suppressed_event_count_above": 0,
        "warn_reset_event_count_above": 0,
        "warn_duplicate_drop_rows_above": 0,
    }


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def load_oracle_policy(policy_path: Path | None) -> dict[str, Any]:
    policy = dict(default_oracle_policy())
    if policy_path is None:
        return policy
    try:
        path = Path(policy_path)
    except (TypeError, ValueError):
        return policy
    try:
        if not path.exists():
            return policy
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            return policy
    except OSError:
        return policy
    except json.JSONDecodeError:
        return policy
    for key, default_value in policy.items():
        if key not in payload:
            continue
        policy[key] = safe_int(payload.get(key, default_value), int(default_value))
    return policy


def write_bug_bundle_if_failed(
    *,
    summary: dict[str, Any],
    state: dict[str, Any],
    new_drop_rows: list[dict[str, Any]],
    new_debug_rows: list[dict[str, Any]],
    policy: dict[str, Any],
    bundle_dir: Path,
    assess_fn: Callable[[dict[str, Any], dict[str, Any]], tuple[bool, list[str], list[str]]],
    extract_focus_event_ids_fn: Callable[[dict[str, Any]], list[str]],
) -> str:
    passed, failures, warnings = assess_fn(summary, policy)
    if passed:
        return ""
    focus_event_ids = set(extract_focus_event_ids_fn(summary))
    if focus_event_ids:
        related_drop_rows = [
            row
            for row in list(new_drop_rows or [])
            if str(row.get("EventID", "") or "").strip() in focus_event_ids
        ]
        related_debug_rows = [
            row
            for row in list(new_debug_rows or [])
            if str(row.get("event_id", "") or "").strip() in focus_event_ids
        ]
    else:
        related_drop_rows = list(new_drop_rows or [])[:80]
        related_debug_rows = list(new_debug_rows or [])[:200]
    bundle_payload = {
        "generated_at_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "failures": failures,
        "warnings": warnings,
        "policy": policy,
        "focus_event_ids": sorted(focus_event_ids),
        "summary": summary,
        "baseline_state": state,
        "related_drop_rows": related_drop_rows[:240],
        "related_debug_rows": related_debug_rows[:500],
    }
    try:
        bundle_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return ""
    bundle_name = f"bundle_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.json"
    bundle_path = bundle_dir / bundle_name
    latest_path = bundle_dir / "latest.json"
    try:
        with bundle_path.open("w", encoding="utf-8") as handle:
            json.dump(bundle_payload, handle, indent=2)
        with latest_path.open("w", encoding="utf-8") as handle:
            json.dump(bundle_payload, handle, indent=2)
    except OSError:
        return ""
    return str(bundle_path)
