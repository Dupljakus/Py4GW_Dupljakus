from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from Sources.oazix.CustomBehaviors.skills.monitoring.drop_event_emitter import append_event_payload_jsonl
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_event_schema import build_event_payload
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_event_schema import normalize_parsed_event_payload
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_live_debug import append_live_debug_log
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_live_debug import format_live_debug_record
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_live_debug import get_live_debug_log_path
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_live_debug import parse_live_debug_line
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_live_debug import read_live_debug_records


def _make_local_temp_dir() -> Path:
    root = Path(".tmp") / "pytest-local"
    root.mkdir(parents=True, exist_ok=True)
    temp_dir = root / f"drop-live-debug-{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=False)
    return temp_dir


def test_read_live_debug_records_tail_and_filters():
    temp_dir = _make_local_temp_dir()
    drop_log_path = temp_dir / "drop_log.csv"
    drop_log_path.write_text("", encoding="utf-8")
    try:
        append_live_debug_log(
            actor="viewer",
            event="viewer_event",
            message="viewer update",
            drop_log_path=str(drop_log_path),
            value=1,
        )
        append_live_debug_log(
            actor="sender",
            event="sender_event",
            message="sender update",
            drop_log_path=str(drop_log_path),
            value=2,
        )
        append_live_debug_log(
            actor="sender",
            event="sender_event_2",
            message="sender update two",
            drop_log_path=str(drop_log_path),
            value=3,
        )

        tail_records = read_live_debug_records(drop_log_path=str(drop_log_path), max_lines=2)
        assert len(tail_records) == 2
        assert tail_records[0]["event"] == "sender_event"
        assert tail_records[1]["event"] == "sender_event_2"

        sender_records = read_live_debug_records(
            drop_log_path=str(drop_log_path),
            max_lines=10,
            actor="sender",
        )
        assert len(sender_records) == 2
        assert all(str(row.get("actor", "")).lower() == "sender" for row in sender_records)

        filtered_records = read_live_debug_records(
            drop_log_path=str(drop_log_path),
            max_lines=10,
            contains_text="two",
        )
        assert len(filtered_records) == 1
        assert filtered_records[0]["event"] == "sender_event_2"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_parse_live_debug_line_handles_non_json_input():
    parsed = parse_live_debug_line("plain-text-entry")
    assert parsed is not None
    assert parsed["actor"] == "raw"
    assert parsed["event"] == "raw"
    assert parsed["message"] == "plain-text-entry"


def test_format_live_debug_record_includes_structured_fields():
    payload = {
        "ts": "2026-03-05 10:00:00.000",
        "actor": "viewer",
        "event": "runtime_update",
        "message": "processed message",
        "count": 7,
        "meta": {"k": "v"},
    }
    line = format_live_debug_record(payload, max_extra_fields=4)
    assert "viewer:runtime_update" in line
    assert "processed message" in line
    assert "count=7" in line
    assert 'meta={"k": "v"}' in line


def test_get_live_debug_log_path_uses_drop_log_directory():
    temp_dir = _make_local_temp_dir()
    try:
        drop_log_path = temp_dir / "drop_log.csv"
        expected = temp_dir / "drop_tracker_live_debug.jsonl"
        resolved = Path(get_live_debug_log_path(str(drop_log_path)))
        assert resolved == expected
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_build_event_payload_normalizes_fields_without_changing_base_shape():
    payload = build_event_payload(
        ts="2026-03-07 19:10:00.000",
        actor="viewer",
        event="viewer_session_reset",
        message="reason=viewer_instance_reset",
        count=3,
        meta={"slot": (1, 2)},
    )

    assert payload["ts"] == "2026-03-07 19:10:00.000"
    assert payload["actor"] == "viewer"
    assert payload["event"] == "viewer_session_reset"
    assert payload["message"] == "reason=viewer_instance_reset"
    assert payload["count"] == 3
    assert payload["meta"] == {"slot": [1, 2]}


def test_append_event_payload_jsonl_writes_compatible_live_debug_row():
    temp_dir = _make_local_temp_dir()
    try:
        target_path = temp_dir / "drop_tracker_live_debug.jsonl"
        append_event_payload_jsonl(
            path=str(target_path),
            ts="2026-03-07 19:11:00.000",
            actor="sender",
            event="sender_session_reset",
            message="transition=instance_change",
            sender_runtime_id="sender-g1-p123",
        )
        rows = read_live_debug_records(log_path=str(target_path), max_lines=10)
        assert len(rows) == 1
        assert rows[0]["actor"] == "sender"
        assert rows[0]["event"] == "sender_session_reset"
        assert rows[0]["sender_runtime_id"] == "sender-g1-p123"
        assert normalize_parsed_event_payload(rows[0])["event"] == "sender_session_reset"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_append_live_debug_log_reserved_log_path_field_does_not_redirect_sink():
    temp_dir = _make_local_temp_dir()
    try:
        drop_log_path = temp_dir / "drop_log.csv"
        debug_log_path = temp_dir / "drop_tracker_live_debug.jsonl"
        append_live_debug_log(
            actor="viewer",
            event="viewer_runtime_heartbeat",
            message="heartbeat",
            drop_log_path=str(drop_log_path),
            log_path=str(drop_log_path),
            live_debug_log_path=str(debug_log_path),
        )

        assert drop_log_path.exists() is False or drop_log_path.read_text(encoding="utf-8") == ""
        rows = read_live_debug_records(log_path=str(debug_log_path), max_lines=10)
        assert len(rows) == 1
        assert rows[0]["event"] == "viewer_runtime_heartbeat"
        assert rows[0]["payload_log_path"] == str(drop_log_path)
        assert rows[0]["live_debug_log_path"] == str(debug_log_path)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
