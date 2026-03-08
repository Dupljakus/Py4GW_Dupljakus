from __future__ import annotations

import csv
import datetime
import json
from pathlib import Path
from typing import Any


PY4GW_ROOT = Path(__file__).resolve().parents[4]
WORK_ROOT = PY4GW_ROOT.parent
DATA_DIR = PY4GW_ROOT / "Py4GW"
DROP_LOG_PATH = DATA_DIR / "drop_log.csv"
LIVE_DEBUG_PATH = DATA_DIR / "drop_tracker_live_debug.jsonl"
RUNTIME_CONFIG_PATH = DATA_DIR / "drop_tracker_runtime_config.json"
STATE_DIR = WORK_ROOT / ".codex_tmp"
STATE_PATH = STATE_DIR / "drop_tracker_live_test_baseline.json"
ORACLE_POLICY_PATH = DATA_DIR / "drop_tracker_live_test_oracle.json"
BUNDLE_DIR = STATE_DIR / "drop_tracker_live_test_bundles"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _is_malformed_csv_row(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict) or None in row:
        return True
    event_id = str(row.get("EventID", "") or "").strip()
    timestamp = str(row.get("Timestamp", "") or "").strip()
    item_name = str(row.get("ItemName", "") or "").strip()
    if timestamp.startswith("{"):
        return True
    if event_id or timestamp or item_name:
        return False
    first_value = next((str(value or "").strip() for value in row.values() if str(value or "").strip()), "")
    return first_value.startswith("{") and '"actor"' in first_value


def row_item_label(row: dict[str, Any]) -> str:
    name = str(row.get("ItemName", "") or row.get("item_name", "") or "Unknown Item").strip()
    qty = _safe_int(row.get("Quantity", row.get("quantity", 1)) or 1, 1)
    rarity = str(row.get("Rarity", row.get("rarity", "Unknown")) or "Unknown").strip()
    player = str(row.get("Player", row.get("sender_name", "")) or "").strip()
    if player:
        return f"{name} x{qty} ({rarity}) [{player}]"
    return f"{name} x{qty} ({rarity})"


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for row in reader:
            parsed = dict(row)
            if _is_malformed_csv_row(parsed):
                continue
            rows.append(parsed)
        return rows


def load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def load_runtime_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def write_state(state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)


def read_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        raise SystemExit("No baseline found. Run `begin`/`arm` first.")
    with STATE_PATH.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise SystemExit("Baseline file is invalid.")
    return payload


def capture_current_state() -> dict[str, Any]:
    drop_rows = load_csv_rows(DROP_LOG_PATH)
    debug_rows = load_jsonl_rows(LIVE_DEBUG_PATH)
    last_drop = drop_rows[-1] if drop_rows else {}
    last_debug = debug_rows[-1] if debug_rows else {}
    return {
        "drop_row_count": len(drop_rows),
        "debug_row_count": len(debug_rows),
        "drop_last_event_id": str(last_drop.get("EventID", "") or "").strip(),
        "drop_last_ts": str(last_drop.get("Timestamp", "") or "").strip(),
        "drop_last_label": row_item_label(last_drop) if isinstance(last_drop, dict) else "",
        "debug_last_ts": str(last_debug.get("ts", "") or "").strip(),
        "debug_last_event": str(last_debug.get("event", "") or "").strip(),
        "debug_last_event_id": str(last_debug.get("event_id", "") or "").strip(),
        "debug_last_message": str(last_debug.get("message", "") or "").strip(),
        "armed_at_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


def begin() -> int:
    state = capture_current_state()
    write_state(state)
    print(
        json.dumps(
            {
                "status": "baseline_ready",
                "drop_row_count": int(state.get("drop_row_count", 0) or 0),
                "debug_row_count": int(state.get("debug_row_count", 0) or 0),
                "state_path": str(STATE_PATH),
            },
            indent=2,
        )
    )
    return 0


def refresh_baseline() -> dict[str, Any]:
    state = capture_current_state()
    write_state(state)
    return state


def slice_drop_rows_since_baseline(state: dict[str, Any], drop_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not drop_rows:
        return []
    baseline_last_event_id = str(state.get("drop_last_event_id", "") or "").strip()
    if baseline_last_event_id:
        for idx in range(len(drop_rows) - 1, -1, -1):
            if str(drop_rows[idx].get("EventID", "") or "").strip() == baseline_last_event_id:
                return list(drop_rows[idx + 1 :])
        return list(drop_rows)
    baseline_count = max(0, int(state.get("drop_row_count", 0) or 0))
    if len(drop_rows) < baseline_count:
        return list(drop_rows)
    return list(drop_rows[baseline_count:])


def slice_debug_rows_since_baseline(state: dict[str, Any], debug_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not debug_rows:
        return []
    marker_ts = str(state.get("debug_last_ts", "") or "").strip()
    marker_event = str(state.get("debug_last_event", "") or "").strip()
    marker_event_id = str(state.get("debug_last_event_id", "") or "").strip()
    marker_message = str(state.get("debug_last_message", "") or "").strip()
    if marker_ts and marker_event:
        for idx in range(len(debug_rows) - 1, -1, -1):
            row = debug_rows[idx]
            if str(row.get("ts", "") or "").strip() != marker_ts:
                continue
            if str(row.get("event", "") or "").strip() != marker_event:
                continue
            if marker_event_id and str(row.get("event_id", "") or "").strip() != marker_event_id:
                continue
            if marker_message and str(row.get("message", "") or "").strip() != marker_message:
                continue
            return list(debug_rows[idx + 1 :])
        return list(debug_rows)
    baseline_count = max(0, int(state.get("debug_row_count", 0) or 0))
    if len(debug_rows) < baseline_count:
        return list(debug_rows)
    return list(debug_rows[baseline_count:])


def slice_rows_since_baseline(
    state: dict[str, Any],
    drop_rows: list[dict[str, Any]],
    debug_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        slice_drop_rows_since_baseline(state, drop_rows),
        slice_debug_rows_since_baseline(state, debug_rows),
    )
