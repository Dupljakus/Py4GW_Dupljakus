from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

from Sources.oazix.CustomBehaviors.tests import drop_test_event_reader as event_reader
from Sources.oazix.CustomBehaviors.tests import drop_tracker_live_test_harness as harness
from Sources.oazix.CustomBehaviors.tests import drop_tracker_live_test_report as report


def _make_local_temp_dir() -> Path:
    root = Path(".tmp") / "pytest-local"
    root.mkdir(parents=True, exist_ok=True)
    temp_dir = root / f"drop-live-test-report-{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=False)
    return temp_dir


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


def test_event_reader_load_csv_rows_skips_malformed_json_tail():
    temp_dir = _make_local_temp_dir()
    try:
        target = temp_dir / "drop_log.csv"
        target.write_text(
            "\n".join(
                [
                    "Timestamp,ViewerBot,MapID,MapName,Player,ItemName,Quantity,Rarity,EventID,ItemStats,ItemID,SenderEmail",
                    "2026-03-08 00:00:01,BotA,93,Spearhead Peak,Player One,Air Wand,1,Gold,ev-1,\"\",101,player1@test",
                    '{"actor":"viewer","event":"viewer_runtime_heartbeat","message":"heartbeat","status_message":"Outpost Store: deposited 1 materials/tomes"}',
                    "2026-03-08 00:00:02,BotA,93,Spearhead Peak,Player Two,Stone Summit Badge,1,White,ev-2,\"\",102,player2@test",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        rows = event_reader.load_csv_rows(target)

        assert [str(row.get("EventID", "")) for row in rows] == ["ev-1", "ev-2"]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


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
            "reset_runtime_count": 1,
            "max_reset_events_per_runtime": 5,
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


def test_assess_summary_warns_with_runtime_distribution_for_partywide_resets():
    passed, failures, warnings = report.assess_summary(
        {
            "accepted_count": 1,
            "new_drop_rows": 1,
            "sent_count": 1,
            "acked_count": 1,
            "send_failed_count": 0,
            "rezone_count": 1,
            "missing_in_csv": [],
            "latest_session_missing_in_csv": [],
            "missing_in_accepted": [],
            "duplicate_drop_rows": [],
            "duplicate_csv_event_ids": [],
            "suspicious_name_update_count": 0,
            "invalid_target_count": 0,
            "duplicate_event_count": 0,
            "suppressed_event_count": 0,
            "reset_event_count": 95,
            "reset_runtime_count": 8,
            "max_reset_events_per_runtime": 13,
        }
    )

    assert passed is True
    assert failures == []
    assert any("across 8 runtimes" in message for message in warnings)


def test_assess_summary_reports_runtime_log_silence_after_reset():
    passed, failures, warnings = report.assess_summary(
        {
            "accepted_count": 0,
            "new_drop_rows": 0,
            "sent_count": 0,
            "acked_count": 0,
            "send_failed_count": 0,
            "rezone_count": 1,
            "missing_in_csv": [],
            "latest_session_missing_in_csv": [],
            "missing_in_accepted": [],
            "duplicate_drop_rows": [],
            "duplicate_csv_event_ids": [],
            "suspicious_name_update_count": 0,
            "invalid_target_count": 0,
            "duplicate_event_count": 0,
            "suppressed_event_count": 0,
            "reset_event_count": 16,
            "reset_runtime_count": 8,
            "max_reset_events_per_runtime": 2,
            "runtime_log_silent_after_reset": True,
        }
    )

    assert passed is False
    assert any("went silent after reset" in message for message in failures)
    assert any("session reset" in message for message in warnings)


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
            "reset_runtime_breakdown": [
                {
                    "actor": "viewer",
                    "runtime_id": "viewer-g1-i1-p123",
                    "count": 4,
                    "latest_reason": "viewer_instance_reset",
                    "latest_caller": "draw_window",
                }
            ],
            "noise_cost_summary": {
                "assessment": "mostly_messy",
                "viewer_reset_perf_count": 4,
                "viewer_reset_perf_avg_ms": 3.25,
                "viewer_reset_perf_max_ms": 7.5,
                "sender_noise_perf_count": 2,
                "sender_noise_perf_avg_ms": 5.5,
                "sender_noise_perf_max_ms": 8.25,
            },
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
    assert "Reset Distribution:" in rendered
    assert "viewer viewer-g1-i1-p123: count=4 latest_reason=viewer_instance_reset caller=draw_window" in rendered
    assert "Noise Cost:" in rendered
    assert "assessment=mostly_messy" in rendered


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


def test_summarize_filters_unknown_route_without_meaningful_signal():
    summary = harness._summarize(
        [],
        [
            {
                "event": "candidate_confirmed",
                "event_id": "ev-orphan",
                "sender_email": "follower@test",
                "item_name": "Bone",
                "ts": "2026-03-05 18:10:00.000",
            }
        ],
    )

    assert list(summary.get("route_lifecycle", []) or []) == []
    assert list(summary.get("receiver_lifecycle", []) or []) == []


def test_summarize_treats_recovered_send_failure_as_warning_not_gap():
    summary = harness._summarize(
        [
            {
                "Timestamp": "2026-03-05 18:10:01.000",
                "EventID": "ev-retry",
                "ItemName": "Insightful Accursed Staff",
                "Quantity": "1",
                "Rarity": "Gold",
                "Player": "Mesmer Jedan",
                "SenderEmail": "sender@test",
                "ItemStats": "Insightful Accursed Staff\nValue: 240 gold",
            }
        ],
        [
            {
                "event": "tracker_transport_target_resolved",
                "event_id": "ev-retry",
                "sender_email": "sender@test",
                "receiver_email": "leader@test",
                "receiver_in_party": True,
                "role": "follower",
                "item_name": "Insightful Accursed Staff",
                "ts": "2026-03-05 18:10:00.000",
            },
            {
                "event": "tracker_drop_send_failed",
                "event_id": "ev-retry",
                "sender_email": "sender@test",
                "receiver_email": "leader@test",
                "item_name": "Insightful Accursed Staff",
                "ts": "2026-03-05 18:10:00.010",
            },
            {
                "event": "tracker_drop_sent",
                "event_id": "ev-retry",
                "sender_email": "sender@test",
                "receiver_email": "leader@test",
                "item_name": "Insightful Accursed Staff",
                "ts": "2026-03-05 18:10:00.020",
            },
            {
                "event": "viewer_drop_accepted",
                "event_id": "ev-retry",
                "sender_email": "sender@test",
                "receiver_email": "leader@test",
                "item_name": "Insightful Accursed Staff",
                "ts": "2026-03-05 18:10:00.030",
            },
            {
                "event": "tracker_drop_acked",
                "event_id": "ev-retry",
                "sender_email": "sender@test",
                "receiver_email": "leader@test",
                "item_name": "Insightful Accursed Staff",
                "ts": "2026-03-05 18:10:00.040",
            },
            {
                "event": "viewer_stats_text_bound",
                "event_id": "ev-retry",
                "sender_email": "sender@test",
                "receiver_email": "leader@test",
                "player_name": "Mesmer Jedan",
                "ts": "2026-03-05 18:10:00.050",
            },
        ],
    )

    assert int(summary.get("send_failed_count", 0) or 0) == 0
    assert int(summary.get("send_failed_raw_count", 0) or 0) == 1
    assert int(summary.get("send_failed_recovered_count", 0) or 0) == 1
    assert int(summary.get("lifecycle_gap_count", 0) or 0) == 0
    lifecycle_rows = list(summary.get("lifecycle_rows", []) or [])
    assert lifecycle_rows[0]["send_failed"] is True
    assert lifecycle_rows[0]["send_failed_unresolved"] is False
    assert lifecycle_rows[0]["send_failed_recovered"] is True
    assert lifecycle_rows[0]["problem_event"] is False
    assert list(summary.get("problem_lifecycle_rows", []) or []) == []
    recovered_rows = list(summary.get("send_failed_recovered_events", []) or [])
    assert len(recovered_rows) == 1
    assert recovered_rows[0]["event_id"] == "ev-retry"


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


def test_assess_summary_warns_on_recovered_send_retry_only():
    passed, failures, warnings = report.assess_summary(
        {
            "accepted_count": 1,
            "new_drop_rows": 1,
            "sent_count": 1,
            "acked_count": 1,
            "send_failed_count": 0,
            "send_failed_recovered_count": 1,
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
        }
    )

    assert passed is True
    assert failures == []
    assert any("recovered on retry" in message for message in warnings)

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


def test_summarize_builds_reset_runtime_breakdown():
    summary = harness._summarize(
        [],
        [
            {
                "actor": "viewer",
                "event": "viewer_session_reset",
                "reason": "viewer_instance_reset",
                "current_map_id": 54,
                "current_instance_uptime_ms": 1200,
                "viewer_runtime_id": "viewer-g1-i1-p123",
                "update_caller": "draw_window",
                "ts": "2026-03-04 19:10:00.000",
            },
            {
                "actor": "viewer",
                "event": "viewer_session_reset",
                "reason": "viewer_instance_reset",
                "current_map_id": 54,
                "current_instance_uptime_ms": 1500,
                "viewer_runtime_id": "viewer-g1-i1-p123",
                "update_caller": "draw_window",
                "ts": "2026-03-04 19:10:01.000",
            },
            {
                "actor": "sender",
                "event": "sender_session_reset",
                "reason": "instance_change",
                "current_map_id": 54,
                "current_instance_uptime_ms": 3100,
                "sender_runtime_id": "sender-g1-p456",
                "reset_source_caller": "run_sender_tick",
                "ts": "2026-03-04 19:10:01.500",
            },
        ],
    )

    assert int(summary.get("reset_event_count", 0) or 0) == 3
    assert int(summary.get("reset_runtime_count", 0) or 0) == 2
    assert int(summary.get("max_reset_events_per_runtime", 0) or 0) == 2
    breakdown = list(summary.get("reset_runtime_breakdown", []) or [])
    assert breakdown[0]["runtime_id"] == "viewer-g1-i1-p123"
    assert int(breakdown[0]["count"] or 0) == 2
    assert breakdown[0]["latest_caller"] == "draw_window"


def test_summarize_builds_noise_cost_summary_from_perf_events():
    summary = harness._summarize(
        [],
        [
            {
                "event": "viewer_reset_perf",
                "duration_ms": 4.0,
                "ts": "2026-03-04 19:10:00.000",
            },
            {
                "event": "viewer_reset_perf",
                "duration_ms": 8.0,
                "ts": "2026-03-04 19:10:01.000",
            },
            {
                "event": "sender_inventory_perf",
                "process_duration_ms": 6.5,
                "suppressed_utility_kit_count": 2,
                "ts": "2026-03-04 19:10:01.500",
            },
        ],
    )

    noise_cost = dict(summary.get("noise_cost_summary", {}) or {})
    assert noise_cost["assessment"] == "mostly_messy"
    assert int(noise_cost["viewer_reset_perf_count"]) == 2
    assert float(noise_cost["viewer_reset_perf_avg_ms"]) == 6.0
    assert float(noise_cost["viewer_reset_perf_max_ms"]) == 8.0
    assert int(noise_cost["sender_noise_perf_count"]) == 1
    assert float(noise_cost["sender_noise_perf_max_ms"]) == 6.5


def test_assess_summary_warns_when_chat_pickup_observer_is_disabled():
    passed, failures, warnings = report.assess_summary(
        {
            "accepted_count": 0,
            "new_drop_rows": 0,
            "send_failed_count": 0,
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
            "reset_runtime_count": 0,
            "max_reset_events_per_runtime": 0,
            "send_failed_recovered_count": 0,
            "rezone_count": 0,
            "forbidden_row_count": 0,
            "lifecycle_gap_count": 0,
            "accepted_missing_stats_binding_count": 0,
            "chat_pickup_count": 0,
            "uncorrelated_chat_pickup_count": 0,
            "chat_item_tracking_enabled": False,
        }
    )

    assert passed is False
    assert failures == ["No tracked drops were captured during the test window."]
    assert "Pickup Watch was OFF for this run; missing chat pickup rows are not evidence that pickups did not happen." in warnings


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
