from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from Sources.oazix.CustomBehaviors.tests import drop_tracker_live_test_harness as harness
from Sources.oazix.CustomBehaviors.tests import drop_tracker_live_test_report as report


def test_assess_summary_passes_clean_run():
    passed, failures, warnings = report.assess_summary(
        {
            "accepted_count": 3,
            "new_drop_rows": 3,
            "sent_count": 3,
            "acked_count": 3,
            "send_failed_count": 0,
            "rezone_count": 0,
            "missing_in_csv": [],
            "latest_session_missing_in_csv": [],
            "missing_in_accepted": [],
            "duplicate_drop_rows": [],
            "duplicate_csv_event_ids": [],
            "suspicious_name_update_count": 0,
            "invalid_target_count": 0,
            "duplicate_event_count": 0,
            "suppressed_event_count": 0,
            "reset_event_count": 0,
        }
    )

    assert passed is True
    assert failures == []
    assert warnings == []


def test_assess_summary_fails_problem_run():
    passed, failures, warnings = report.assess_summary(
        {
            "accepted_count": 1,
            "new_drop_rows": 1,
            "sent_count": 2,
            "acked_count": 0,
            "send_failed_count": 2,
            "rezone_count": 1,
            "missing_in_csv": ["Bog Skale Fin x1 (White)"],
            "latest_session_missing_in_csv": [],
            "missing_in_accepted": [],
            "duplicate_drop_rows": ["Bog Skale Fin x1 (White)"],
            "duplicate_csv_event_ids": ["ev-1"],
            "suspicious_name_update_count": 1,
            "invalid_target_count": 1,
            "duplicate_event_count": 1,
            "suppressed_event_count": 3,
            "reset_event_count": 5,
        }
    )

    assert passed is False
    assert any("Transport failed" in message for message in failures)
    assert any("accepted drop" in message for message in failures)
    assert any("duplicate CSV event id" in message for message in failures)
    assert any("suspicious late row rename" in message for message in failures)
    assert any("outside current party members" in message for message in failures)
    assert any("duplicate viewer event" in message for message in warnings)
    assert any("candidate suppression" in message for message in warnings)
    assert any("session reset" in message for message in warnings)
    assert any("repeated loot-table label" in message for message in warnings)


def test_assess_summary_fails_forbidden_rows_and_lifecycle_gaps():
    passed, failures, _warnings = report.assess_summary(
        {
            "accepted_count": 2,
            "new_drop_rows": 2,
            "sent_count": 2,
            "acked_count": 2,
            "send_failed_count": 0,
            "rezone_count": 0,
            "missing_in_csv": [],
            "latest_session_missing_in_csv": [],
            "missing_in_accepted": [],
            "duplicate_drop_rows": [],
            "duplicate_csv_event_ids": [],
            "suspicious_name_update_count": 0,
            "stats_name_mismatch_count": 0,
            "invalid_target_count": 0,
            "forbidden_row_count": 1,
            "lifecycle_gap_count": 2,
            "accepted_missing_stats_binding_count": 1,
            "duplicate_event_count": 0,
            "suppressed_event_count": 0,
            "reset_event_count": 0,
        }
    )

    assert passed is False
    assert any("forbidden loot-table row" in message.lower() for message in failures)
    assert any("lifecycle gap" in message.lower() for message in failures)
    assert any("missing bound stats" in message.lower() for message in failures)


def test_format_report_includes_recent_problem_details():
    rendered = report.format_report(
        {
            "accepted_count": 0,
            "new_drop_rows": 0,
            "sent_count": 1,
            "acked_count": 0,
            "send_failed_count": 1,
            "rezone_count": 1,
            "missing_in_csv": [],
            "latest_session_missing_in_csv": [],
            "missing_in_accepted": [],
            "duplicate_drop_rows": [],
            "duplicate_csv_event_ids": [],
            "suspicious_name_update_count": 1,
            "stats_name_mismatch_count": 1,
            "invalid_target_count": 1,
            "duplicate_event_count": 0,
            "suppressed_event_count": 0,
            "reset_event_count": 0,
            "suspicious_name_updates": [
                {"previous_name": "Bog Skale Fin", "new_name": "Necromancer Tome"}
            ],
            "stats_name_mismatches": [
                {
                    "row_names_after": ["The Flameseeker Prophecies [Hard Mode]"],
                    "payload_name": "Deadly Cesta of Quickening",
                }
            ],
            "send_failed_events": [
                {"item_name": "Half-Eaten Blob", "receiver_email": "leader@test"}
            ],
            "invalid_target_events": [
                {"sender_email": "follower@test", "receiver_email": "stale@test"}
            ],
            "receiver_lifecycle": [
                {
                    "receiver_email": "leader@test",
                    "accepted": 1,
                    "csv": 0,
                    "sent_missing_accepted": 0,
                    "accepted_missing_csv": 1,
                }
            ],
            "route_lifecycle": [
                {
                    "sender_email": "follower@test",
                    "receiver_email": "leader@test",
                    "events": 1,
                    "sent": 1,
                    "accepted": 0,
                    "acked": 0,
                    "csv": 0,
                    "problem_events": 1,
                }
            ],
            "problem_lifecycle_rows": [
                {
                    "event_id": "ev-problem",
                    "sender_email": "follower@test",
                    "receiver_email": "leader@test",
                    "label": "Half-Eaten Blob x1 (White) [Follower]",
                    "gap_codes": ["sent_missing_accepted"],
                    "owner_hints": ["receiver"],
                }
            ],
            "rezones": [
                {
                    "current_map_id": 54,
                    "current_instance_uptime_ms": 1310,
                    "ts": "2026-03-04 19:10:00.000",
                    "reasons": ["viewer_instance_reset", "viewer_sync_reset"],
                }
            ],
        }
    )

    assert "LIVE TEST FAIL" in rendered
    assert "Rezones Detected: 1" in rendered
    assert "Bog Skale Fin -> Necromancer Tome" in rendered
    assert "row=The Flameseeker Prophecies [Hard Mode] bound=Deadly Cesta of Quickening" in rendered
    assert "Half-Eaten Blob -> leader@test" in rendered
    assert "follower@test -> stale@test" in rendered
    assert "leader@test: accepted=1 csv=0 sent_missing_accepted=0 accepted_missing_csv=1" in rendered
    assert "follower@test -> leader@test: events=1 sent=1 accepted=0 acked=0 csv=0 problems=1" in rendered
    assert "ev=ev-problem follower@test -> leader@test gaps=sent_missing_accepted sides=receiver" in rendered
    assert "map=54 uptime_ms=1310" in rendered


def test_summarize_detects_forbidden_kit_rows_and_lifecycle_gap():
    summary = harness._summarize(
        [
            {
                "EventID": "ev-kit",
                "ItemName": "Salvage Kit",
                "Quantity": "1",
                "Rarity": "White",
                "Player": "Player Three",
                "SenderEmail": "sender@test",
                "ModelID": "239",
                "ItemStats": "",
            }
        ],
        [],
    )

    assert int(summary.get("forbidden_row_count", 0) or 0) == 1
    assert int(summary.get("lifecycle_gap_count", 0) or 0) >= 1
    assert any(
        str(row.get("code", "") or "").strip() == "csv_missing_accepted"
        for row in list(summary.get("lifecycle_gaps", []) or [])
    )


def test_summarize_tracks_sender_receiver_route_and_problem_side():
    summary = harness._summarize(
        [],
        [
            {
                "event": "tracker_transport_target_resolved",
                "event_id": "ev-route-1",
                "sender_email": "follower@test",
                "receiver_email": "leader@test",
                "receiver_in_party": True,
                "role": "follower",
                "item_name": "Bone",
                "ts": "2026-03-05 18:10:00.000",
            },
            {
                "event": "tracker_drop_sent",
                "event_id": "ev-route-1",
                "sender_email": "follower@test",
                "receiver_email": "leader@test",
                "item_name": "Bone",
                "ts": "2026-03-05 18:10:00.010",
            },
        ],
    )

    lifecycle_rows = list(summary.get("lifecycle_rows", []) or [])
    assert len(lifecycle_rows) == 1
    assert lifecycle_rows[0]["receiver_email"] == "leader@test"
    assert lifecycle_rows[0]["target_resolved"] is True
    assert lifecycle_rows[0]["problem_event"] is True
    assert "sent_missing_accepted" in list(lifecycle_rows[0].get("gap_codes", []) or [])

    receiver_lifecycle = list(summary.get("receiver_lifecycle", []) or [])
    assert receiver_lifecycle[0]["receiver_email"] == "leader@test"
    assert int(receiver_lifecycle[0]["sent_missing_accepted"] or 0) == 1

    route_lifecycle = list(summary.get("route_lifecycle", []) or [])
    assert route_lifecycle[0]["sender_email"] == "follower@test"
    assert route_lifecycle[0]["receiver_email"] == "leader@test"
    assert int(route_lifecycle[0]["problem_events"] or 0) == 1

    problem_rows = list(summary.get("problem_lifecycle_rows", []) or [])
    assert problem_rows[0]["primary_owner_hint"] == "receiver"


def test_write_bug_bundle_if_failed_creates_artifact(tmp_path, monkeypatch):
    bundle_dir = tmp_path / "bundles"
    monkeypatch.setattr(report.harness, "BUNDLE_DIR", bundle_dir, raising=False)

    summary = {
        "accepted_count": 1,
        "new_drop_rows": 1,
        "sent_count": 1,
        "acked_count": 0,
        "send_failed_count": 1,
        "rezone_count": 0,
        "missing_in_csv": [],
        "latest_session_missing_in_csv": [],
        "missing_in_accepted": [],
        "duplicate_drop_rows": [],
        "duplicate_csv_event_ids": [],
        "suspicious_name_update_count": 0,
        "stats_name_mismatch_count": 0,
        "invalid_target_count": 0,
        "forbidden_row_count": 0,
        "lifecycle_gap_count": 0,
        "accepted_missing_stats_binding_count": 0,
        "duplicate_event_count": 0,
        "suppressed_event_count": 0,
        "reset_event_count": 0,
        "send_failed_events": [{"event_id": "ev-1", "item_name": "Half-Eaten Blob"}],
    }
    path = report._write_bug_bundle_if_failed(
        summary=summary,
        state={"drop_row_count": 1, "debug_row_count": 1},
        new_drop_rows=[{"EventID": "ev-1", "ItemName": "Half-Eaten Blob"}],
        new_debug_rows=[{"event": "tracker_drop_send_failed", "event_id": "ev-1"}],
        policy=report._default_oracle_policy(),
    )

    assert path
    artifact_path = bundle_dir / Path(path).name
    assert artifact_path.exists()

def test_collect_likely_rezones_clusters_reset_churn():
    rezones = harness._collect_likely_rezones(
        [
            {
                "event": "viewer_session_reset",
                "reason": "viewer_instance_reset",
                "current_map_id": 54,
                "current_instance_uptime_ms": 1200,
                "ts": "2026-03-04 19:10:00.000",
            },
            {
                "event": "sender_session_reset",
                "reason": "viewer_sync_reset",
                "current_map_id": 54,
                "current_instance_uptime_ms": 1450,
                "ts": "2026-03-04 19:10:01.000",
            },
            {
                "event": "sender_session_reset",
                "reason": "instance_change",
                "current_map_id": 14,
                "current_instance_uptime_ms": 1100,
                "ts": "2026-03-04 19:12:30.000",
            },
        ]
    )

    assert len(rezones) == 2
    assert rezones[0]["current_map_id"] == 54
    assert rezones[0]["reasons"] == ["viewer_instance_reset", "viewer_sync_reset"]
    assert rezones[1]["current_map_id"] == 14
    assert rezones[1]["reasons"] == ["instance_change"]


def test_collect_likely_rezones_clusters_high_uptime_and_startup_resets_on_same_map():
    rezones = harness._collect_likely_rezones(
        [
            {
                "event": "viewer_session_reset",
                "reason": "viewer_instance_reset",
                "current_map_id": 54,
                "current_instance_uptime_ms": 113731,
                "ts": "2026-03-04 19:21:21.813",
            },
            {
                "event": "sender_session_reset",
                "reason": "viewer_sync_reset",
                "current_map_id": 54,
                "current_instance_uptime_ms": 1526,
                "ts": "2026-03-04 19:21:22.041",
            },
        ]
    )

    assert len(rezones) == 1
    assert rezones[0]["current_map_id"] == 54
    assert rezones[0]["reasons"] == ["viewer_instance_reset", "viewer_sync_reset"]


def test_summarize_keeps_csv_lifecycle_gaps_across_rezones():
    summary = harness._summarize(
        [
            {
                "Timestamp": "2026-03-05 18:10:00.000",
                "EventID": "ev-old-csv",
                "ItemName": "Old Item",
                "Quantity": "1",
                "Rarity": "White",
                "Player": "Player Three",
                "SenderEmail": "sender@test",
                "ItemStats": "Unidentified",
            },
            {
                "Timestamp": "2026-03-05 18:20:00.000",
                "EventID": "ev-new-csv",
                "ItemName": "New Item",
                "Quantity": "1",
                "Rarity": "White",
                "Player": "Player Three",
                "SenderEmail": "sender@test",
                "ItemStats": "Unidentified",
            },
        ],
        [
            {
                "event": "viewer_drop_accepted",
                "event_id": "ev-old-missing",
                "sender_email": "sender@test",
                "ts": "2026-03-05 18:11:00.000",
            },
            {
                "event": "viewer_session_reset",
                "reason": "viewer_instance_reset",
                "current_map_id": 92,
                "current_instance_uptime_ms": 1200,
                "ts": "2026-03-05 18:19:00.000",
            },
            {
                "event": "viewer_drop_accepted",
                "event_id": "ev-new-csv",
                "sender_email": "sender@test",
                "ts": "2026-03-05 18:20:01.000",
            },
        ],
    )

    lifecycle_gaps = list(summary.get("lifecycle_gaps", []) or [])
    assert int(summary.get("rezone_count", 0) or 0) == 1
    assert any(
        str(row.get("event_id", "")).strip() == "ev-old-missing"
        and str(row.get("code", "")).strip() == "accepted_missing_csv"
        for row in lifecycle_gaps
    )

def test_summarize_does_not_flag_safe_full_name_enrichment_as_suspicious():
    summary = harness._summarize(
        [],
        [
            {
                "event": "viewer_row_name_updated",
                "event_id": "ev-1",
                "player_name": "Player Five",
                "sender_email": "sender@test",
                "rarity": "Gold",
                "previous_name": "Longbow",
                "new_name": "Shocking Longbow of Tenguslaying",
                "previous_was_unknown": False,
                "update_source": "signature_and_sender",
            }
        ],
    )

    assert summary["row_name_update_count"] == 1
    assert summary["suspicious_name_update_count"] == 0


def test_slice_drop_rows_since_baseline_handles_log_reset_by_missing_anchor():
    state = {
        "drop_row_count": 4,
        "drop_last_event_id": "old-ev-4",
    }
    current_rows = [
        {"EventID": "new-ev-1", "ItemName": "Holy Staff", "Quantity": "1", "Rarity": "White", "Player": "Player Three"},
        {"EventID": "new-ev-2", "ItemName": "Bone Staff", "Quantity": "1", "Rarity": "Blue", "Player": "Player Seven"},
    ]

    new_rows = harness._slice_drop_rows_since_baseline(state, current_rows)

    assert [str(row.get("EventID", "")) for row in new_rows] == ["new-ev-1", "new-ev-2"]


def test_slice_debug_rows_since_baseline_handles_truncated_log_with_missing_marker():
    state = {
        "debug_row_count": 1000,
        "debug_last_ts": "2026-03-05 02:10:00.000",
        "debug_last_event": "viewer_drop_accepted",
        "debug_last_event_id": "ev-old",
        "debug_last_message": "event_id=ev-old",
    }
    current_rows = [
        {"ts": "2026-03-05 02:11:00.000", "event": "viewer_drop_accepted", "event_id": "ev-new-1", "message": "event_id=ev-new-1"},
        {"ts": "2026-03-05 02:11:01.000", "event": "viewer_drop_accepted", "event_id": "ev-new-2", "message": "event_id=ev-new-2"},
    ]

    new_rows = harness._slice_debug_rows_since_baseline(state, current_rows)

    assert [str(row.get("event_id", "")) for row in new_rows] == ["ev-new-1", "ev-new-2"]


def test_auto_mode_arms_baseline_when_missing(monkeypatch, capsys):
    begin_called = {"count": 0}

    monkeypatch.setattr(report, "_baseline_exists", lambda: False)
    monkeypatch.setattr(
        report,
        "harness",
        SimpleNamespace(
            _begin=lambda: begin_called.__setitem__("count", begin_called["count"] + 1) or 0,
        ),
    )

    rc = report._auto()
    output = capsys.readouterr().out

    assert rc == 0
    assert begin_called["count"] == 1
    assert "baseline armed" in output.lower()


def test_auto_mode_reports_and_refreshes_when_new_rows(monkeypatch, capsys):
    refreshed = {"count": 0}
    fake_summary = {
        "accepted_count": 2,
        "new_drop_rows": 2,
        "sent_count": 2,
        "acked_count": 2,
        "send_failed_count": 0,
        "rezone_count": 0,
        "missing_in_csv": [],
        "latest_session_missing_in_csv": [],
        "missing_in_accepted": [],
        "duplicate_drop_rows": [],
        "duplicate_csv_event_ids": [],
        "suspicious_name_update_count": 0,
        "stats_name_mismatch_count": 0,
        "invalid_target_count": 0,
        "duplicate_event_count": 0,
        "suppressed_event_count": 0,
        "reset_event_count": 0,
    }

    fake_harness = SimpleNamespace(
        DROP_LOG_PATH="drop.csv",
        LIVE_DEBUG_PATH="debug.jsonl",
        _read_state=lambda: {"drop_row_count": 0, "debug_row_count": 0},
        _load_csv_rows=lambda _path: [{"EventID": "ev-1"}],
        _load_jsonl_rows=lambda _path: [{"event": "viewer_drop_accepted", "event_id": "ev-1"}],
        _summarize=lambda _drops, _debug: dict(fake_summary),
        _refresh_baseline=lambda: refreshed.__setitem__("count", refreshed["count"] + 1),
    )

    monkeypatch.setattr(report, "_baseline_exists", lambda: True)
    monkeypatch.setattr(report, "harness", fake_harness)
    monkeypatch.setattr(report, "format_report", lambda _summary: "LIVE TEST PASS")

    rc = report._auto()
    output = capsys.readouterr().out

    assert rc == 0
    assert refreshed["count"] == 1
    assert "LIVE TEST PASS" in output


def test_auto_mode_no_new_rows_returns_zero(monkeypatch, capsys):
    refreshed = {"count": 0}
    fake_harness = SimpleNamespace(
        DROP_LOG_PATH="drop.csv",
        LIVE_DEBUG_PATH="debug.jsonl",
        _read_state=lambda: {"drop_row_count": 2, "debug_row_count": 3},
        _load_csv_rows=lambda _path: [{"EventID": "ev-1"}, {"EventID": "ev-2"}],
        _load_jsonl_rows=lambda _path: [{"event": "a"}, {"event": "b"}, {"event": "c"}],
        _summarize=lambda _drops, _debug: {},
        _refresh_baseline=lambda: refreshed.__setitem__("count", refreshed["count"] + 1),
    )

    monkeypatch.setattr(report, "_baseline_exists", lambda: True)
    monkeypatch.setattr(report, "harness", fake_harness)

    rc = report._auto()
    output = capsys.readouterr().out

    assert rc == 0
    assert refreshed["count"] == 0
    assert "no new tracker/debug rows" in output.lower()
