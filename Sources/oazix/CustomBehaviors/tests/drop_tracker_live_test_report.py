from __future__ import annotations

import datetime
import json
import sys
from pathlib import Path
from typing import Any

from Sources.oazix.CustomBehaviors.tests import drop_test_reporting as reporting

try:
    from Sources.oazix.CustomBehaviors.tests import (
        drop_tracker_live_test_harness as harness,
    )
except ImportError:
    import drop_tracker_live_test_harness as harness


def _default_oracle_policy() -> dict[str, Any]:
    return reporting.default_oracle_policy()


def _safe_int(value: Any, default: int = 0) -> int:
    return reporting.safe_int(value, default)


def _load_oracle_policy() -> dict[str, Any]:
    path_value = getattr(harness, "ORACLE_POLICY_PATH", None)
    try:
        path = Path(path_value) if path_value is not None else None
    except (TypeError, ValueError):
        path = None
    return reporting.load_oracle_policy(path)


def assess_summary(summary: dict[str, Any], policy: dict[str, Any] | None = None) -> tuple[bool, list[str], list[str]]:
    config = dict(policy or _load_oracle_policy())
    failures: list[str] = []
    warnings: list[str] = []

    send_failed_count = int(summary.get("send_failed_count", 0) or 0)
    missing_in_csv = list(summary.get("missing_in_csv", []))
    latest_session_missing_in_csv = list(summary.get("latest_session_missing_in_csv", []))
    missing_in_accepted = list(summary.get("missing_in_accepted", []))
    duplicate_drop_rows = list(summary.get("duplicate_drop_rows", []))
    duplicate_csv_event_ids = list(summary.get("duplicate_csv_event_ids", []))
    suspicious_name_update_count = int(summary.get("suspicious_name_update_count", 0) or 0)
    stats_name_mismatch_count = int(summary.get("stats_name_mismatch_count", 0) or 0)
    invalid_target_count = int(summary.get("invalid_target_count", 0) or 0)
    accepted_count = int(summary.get("accepted_count", 0) or 0)
    new_drop_rows = int(summary.get("new_drop_rows", 0) or 0)
    duplicate_event_count = int(summary.get("duplicate_event_count", 0) or 0)
    suppressed_event_count = int(summary.get("suppressed_event_count", 0) or 0)
    reset_event_count = int(summary.get("reset_event_count", 0) or 0)
    reset_runtime_count = int(summary.get("reset_runtime_count", 0) or 0)
    max_reset_events_per_runtime = int(summary.get("max_reset_events_per_runtime", 0) or 0)
    recovered_send_failed_count = int(summary.get("send_failed_recovered_count", 0) or 0)
    rezone_count = int(summary.get("rezone_count", 0) or 0)
    forbidden_row_count = int(summary.get("forbidden_row_count", 0) or 0)
    lifecycle_gap_count = int(summary.get("lifecycle_gap_count", 0) or 0)
    accepted_missing_stats_binding_count = int(summary.get("accepted_missing_stats_binding_count", 0) or 0)
    chat_pickup_count = int(summary.get("chat_pickup_count", 0) or 0)
    uncorrelated_chat_pickup_count = int(summary.get("uncorrelated_chat_pickup_count", 0) or 0)
    chat_item_tracking_enabled = bool(summary.get("chat_item_tracking_enabled", False))

    if send_failed_count > int(config.get("max_send_failed_count", 0) or 0):
        failures.append(f"Transport failed for {send_failed_count} tracker send attempt(s).")
    if len(missing_in_csv) > int(config.get("max_missing_in_csv", 0) or 0):
        failures.append(f"{len(missing_in_csv)} accepted drop(s) never reached drop_log.csv.")
    if len(missing_in_accepted) > int(config.get("max_missing_in_accepted", 0) or 0):
        failures.append(f"{len(missing_in_accepted)} CSV row(s) have no matching accepted viewer event.")
    if len(duplicate_csv_event_ids) > int(config.get("max_duplicate_csv_event_ids", 0) or 0):
        failures.append(f"{len(duplicate_csv_event_ids)} duplicate CSV event id(s) were recorded.")
    if suspicious_name_update_count > int(config.get("max_suspicious_name_update_count", 0) or 0):
        failures.append(
            f"{suspicious_name_update_count} suspicious late row rename(s) were detected."
        )
    if stats_name_mismatch_count > int(config.get("max_stats_name_mismatch_count", 0) or 0):
        failures.append(f"{stats_name_mismatch_count} stats/name binding mismatch event(s) were detected.")
    if invalid_target_count > int(config.get("max_invalid_target_count", 0) or 0):
        failures.append(f"{invalid_target_count} drop transport target(s) were outside current party members.")
    if forbidden_row_count > int(config.get("max_forbidden_row_count", 0) or 0):
        failures.append(f"{forbidden_row_count} forbidden loot-table row(s) were detected.")
    if lifecycle_gap_count > int(config.get("max_lifecycle_gap_count", 0) or 0):
        failures.append(f"{lifecycle_gap_count} event lifecycle gap(s) were detected.")
    if accepted_missing_stats_binding_count > int(config.get("max_accepted_missing_stats_binding_count", 0) or 0):
        failures.append(
            f"{accepted_missing_stats_binding_count} accepted drop(s) are missing bound stats."
        )
    if accepted_count == 0 and new_drop_rows == 0:
        if bool(summary.get("window_ended_before_post_reset_sender_cycle", False)):
            failures.append("No tracked drops were captured; the test window ended before any post-reset sender cycle ran.")
        elif bool(summary.get("window_ended_during_sender_startup", False)):
            failures.append("No tracked drops were captured; the test window ended during sender startup after reset.")
        elif bool(summary.get("runtime_log_silent_after_reset", False)):
            failures.append("No tracked drops were captured; the analyzed runtime log went silent after reset.")
        elif uncorrelated_chat_pickup_count > 0:
            failures.append(
                f"{uncorrelated_chat_pickup_count} chat pickup(s) were observed without any tracked sender/viewer event."
            )
        else:
            failures.append("No tracked drops were captured during the test window.")
    elif uncorrelated_chat_pickup_count > 0:
        failures.append(
            f"{uncorrelated_chat_pickup_count} chat pickup(s) were observed without any tracked sender/viewer event."
        )

    if duplicate_event_count > int(config.get("warn_duplicate_event_count_above", 0) or 0):
        warnings.append(f"{duplicate_event_count} duplicate viewer event(s) were observed.")
    if suppressed_event_count > int(config.get("warn_suppressed_event_count_above", 0) or 0):
        warnings.append(f"{suppressed_event_count} candidate suppression event(s) were observed.")
    if reset_event_count > int(config.get("warn_reset_event_count_above", 0) or 0):
        if reset_runtime_count > 1:
            warnings.append(
                f"{reset_event_count} session reset event(s) occurred across {reset_runtime_count} runtimes "
                f"(max {max_reset_events_per_runtime} on one runtime)."
            )
        else:
            warnings.append(f"{reset_event_count} session reset event(s) occurred during the run.")
    if len(duplicate_drop_rows) > int(config.get("warn_duplicate_drop_rows_above", 0) or 0):
        warnings.append(f"{len(duplicate_drop_rows)} repeated loot-table label(s) were observed.")
    if recovered_send_failed_count > 0:
        warnings.append(f"{recovered_send_failed_count} tracker send failure(s) recovered on retry.")
    if chat_pickup_count > 0 and uncorrelated_chat_pickup_count == 0:
        warnings.append(f"{chat_pickup_count} chat pickup(s) correlated with tracked events.")
    if not chat_item_tracking_enabled and accepted_count == 0 and new_drop_rows == 0:
        warnings.append("Pickup Watch was OFF for this run; missing chat pickup rows are not evidence that pickups did not happen.")

    return not failures, failures, warnings


def format_report(summary: dict[str, Any]) -> str:
    passed, failures, warnings = assess_summary(summary)
    lines = [
        f"LIVE TEST {'PASS' if passed else 'FAIL'}",
        (
            "Accepted Events: "
            f"{int(summary.get('accepted_count', 0) or 0)} | "
            f"CSV Rows: {int(summary.get('new_drop_rows', 0) or 0)} | "
            f"Sent: {int(summary.get('sent_count', 0) or 0)} | "
            f"Acked: {int(summary.get('acked_count', 0) or 0)} | "
            f"Send Failed: {int(summary.get('send_failed_count', 0) or 0)}"
        ),
        f"Rezones Detected: {int(summary.get('rezone_count', 0) or 0)}",
    ]
    missing_in_csv = list(summary.get("missing_in_csv", []))
    latest_session_missing_in_csv = list(summary.get("latest_session_missing_in_csv", []))
    if missing_in_csv:
        lines.append(f"Missing In CSV: {len(missing_in_csv)}")
    if latest_session_missing_in_csv:
        lines.append(f"Latest Session Missing In CSV: {len(latest_session_missing_in_csv)}")

    if failures:
        lines.append("Failures:")
        lines.extend(f"- {message}" for message in failures)
    if warnings:
        lines.append("Warnings:")
        lines.extend(f"- {message}" for message in warnings)

    suspicious_name_updates = list(summary.get("suspicious_name_updates", []))
    if suspicious_name_updates:
        lines.append("Suspicious Renames:")
        for row in suspicious_name_updates[:5]:
            previous_name = str(row.get("previous_name", "") or "").strip() or "Unknown"
            new_name = str(row.get("new_name", "") or "").strip() or "Unknown"
            lines.append(f"- {previous_name} -> {new_name}")

    stats_name_mismatches = list(summary.get("stats_name_mismatches", []))
    if stats_name_mismatches:
        lines.append("Stats/Name Mismatches:")
        for row in stats_name_mismatches[:5]:
            payload_name = str(row.get("payload_name", "") or row.get("first_line_name", "") or "").strip() or "Unknown"
            row_names = list(row.get("row_names_after", []) or row.get("row_names_before", []) or [])
            row_name = str(row_names[0] if row_names else "Unknown").strip()
            lines.append(f"- row={row_name} bound={payload_name}")

    send_failed_events = list(summary.get("send_failed_events", []))
    if send_failed_events:
        lines.append("Recent Send Failures:")
        for row in send_failed_events[:5]:
            item_name = str(row.get("label", "") or row.get("item_name", "") or "Unknown Item").strip()
            receiver = str(row.get("receiver_email", "") or "Unknown Receiver").strip()
            lines.append(f"- {item_name} -> {receiver}")

    recovered_send_failed_events = list(summary.get("send_failed_recovered_events", []))
    if recovered_send_failed_events:
        lines.append("Recovered Send Retries:")
        for row in recovered_send_failed_events[:5]:
            item_name = str(row.get("label", "") or row.get("item_name", "") or "Unknown Item").strip()
            receiver = str(row.get("receiver_email", "") or "Unknown Receiver").strip()
            lines.append(f"- {item_name} -> {receiver}")

    invalid_target_events = list(summary.get("invalid_target_events", []))
    if invalid_target_events:
        lines.append("Invalid Transport Targets:")
        for row in invalid_target_events[:5]:
            sender = str(row.get("sender_email", "") or "unknown-sender").strip()
            receiver = str(row.get("receiver_email", "") or "unknown-receiver").strip()
            lines.append(f"- {sender} -> {receiver}")

    forbidden_rows = list(summary.get("forbidden_rows", []))
    if forbidden_rows:
        lines.append("Forbidden Loot Rows:")
        for row in forbidden_rows[:8]:
            label = str(row.get("label", "") or "").strip() or "Unknown Item"
            model_id = int(row.get("model_id", 0) or 0)
            lines.append(f"- model={model_id} {label}")

    accepted_missing_stats_binding = list(summary.get("accepted_missing_stats_binding", []))
    if accepted_missing_stats_binding:
        lines.append("Accepted Missing Stats Binding:")
        for row in accepted_missing_stats_binding[:8]:
            label = str(row.get("label", "") or "").strip() or "Unknown Item"
            event_id = str(row.get("event_id", "") or "").strip() or "-"
            lines.append(f"- ev={event_id} {label}")

    lifecycle_gaps = list(summary.get("lifecycle_gaps", []))
    if lifecycle_gaps:
        lines.append("Lifecycle Gaps:")
        for row in lifecycle_gaps[:10]:
            code = str(row.get("code", "") or "").strip() or "unknown"
            event_id = str(row.get("event_id", "") or "").strip() or "-"
            label = str(row.get("label", "") or "").strip() or "Unknown Item"
            owner_hint = str(row.get("owner_hint", "") or "").strip() or "unknown"
            sender = str(row.get("sender_email", "") or "").strip() or "unknown-sender"
            receiver = str(row.get("receiver_email", "") or "").strip() or "unknown-receiver"
            lines.append(
                f"- {code} side={owner_hint} ev={event_id} {sender} -> {receiver} {label}"
            )

    sender_lifecycle = list(summary.get("sender_lifecycle", []))
    if sender_lifecycle:
        lines.append("Per-Sender Lifecycle:")
        for row in sender_lifecycle[:8]:
            sender_email = str(row.get("sender_email", "") or "").strip() or "unknown"
            accepted = int(row.get("accepted", 0) or 0)
            csv = int(row.get("csv", 0) or 0)
            stats_missing = int(row.get("accepted_missing_stats_binding", 0) or 0)
            problems = int(row.get("problem_events", 0) or 0)
            lines.append(
                f"- {sender_email}: accepted={accepted} csv={csv} stats_missing={stats_missing} problems={problems}"
            )

    receiver_lifecycle = list(summary.get("receiver_lifecycle", []))
    if receiver_lifecycle:
        lines.append("Per-Receiver Lifecycle:")
        for row in receiver_lifecycle[:8]:
            receiver_email = str(row.get("receiver_email", "") or "").strip() or "unknown"
            accepted = int(row.get("accepted", 0) or 0)
            csv = int(row.get("csv", 0) or 0)
            sent_missing_accepted = int(row.get("sent_missing_accepted", 0) or 0)
            accepted_missing_csv = int(row.get("accepted_missing_csv", 0) or 0)
            lines.append(
                f"- {receiver_email}: accepted={accepted} csv={csv} sent_missing_accepted={sent_missing_accepted} accepted_missing_csv={accepted_missing_csv}"
            )

    route_lifecycle = list(summary.get("route_lifecycle", []))
    if route_lifecycle:
        lines.append("Sender/Receiver Routes:")
        for row in route_lifecycle[:10]:
            sender_email = str(row.get("sender_email", "") or "").strip() or "unknown-sender"
            receiver_email = str(row.get("receiver_email", "") or "").strip() or "unknown-receiver"
            events = int(row.get("events", 0) or 0)
            sent = int(row.get("sent", 0) or 0)
            accepted = int(row.get("accepted", 0) or 0)
            acked = int(row.get("acked", 0) or 0)
            csv = int(row.get("csv", 0) or 0)
            problems = int(row.get("problem_events", 0) or 0)
            lines.append(
                f"- {sender_email} -> {receiver_email}: events={events} sent={sent} accepted={accepted} acked={acked} csv={csv} problems={problems}"
            )

    problem_lifecycle_rows = list(summary.get("problem_lifecycle_rows", []))
    if problem_lifecycle_rows:
        lines.append("Problem Events:")
        for row in problem_lifecycle_rows[:12]:
            event_id = str(row.get("event_id", "") or "").strip() or "-"
            sender_email = str(row.get("sender_email", "") or "").strip() or "unknown-sender"
            receiver_email = str(row.get("receiver_email", "") or "").strip() or "unknown-receiver"
            label = str(row.get("label", "") or "").strip() or "Unknown Item"
            gap_codes = [
                str(value or "").strip()
                for value in list(row.get("gap_codes", []) or [])
                if str(value or "").strip()
            ]
            owner_hints = [
                str(value or "").strip()
                for value in list(row.get("owner_hints", []) or [])
                if str(value or "").strip()
            ]
            lines.append(
                f"- ev={event_id} {sender_email} -> {receiver_email} gaps={','.join(gap_codes) or 'unknown'} sides={','.join(owner_hints) or 'unknown'} {label}"
            )

    rezones = list(summary.get("rezones", []))
    if rezones:
        lines.append("Rezones:")
        for row in rezones[:5]:
            map_id = int(row.get("current_map_id", 0) or 0)
            uptime_ms = int(row.get("current_instance_uptime_ms", 0) or 0)
            ts_value = str(row.get("ts", "") or "").strip() or "unknown-ts"
            reasons = ",".join(str(value or "").strip() for value in list(row.get("reasons", []) or []) if str(value or "").strip())
            lines.append(f"- map={map_id} uptime_ms={uptime_ms} ts={ts_value} reasons={reasons or 'unknown'}")

    reset_runtime_breakdown = list(summary.get("reset_runtime_breakdown", []))
    if reset_runtime_breakdown:
        lines.append("Reset Distribution:")
        for row in reset_runtime_breakdown[:8]:
            actor = str(row.get("actor", "") or "").strip() or "unknown"
            runtime_id = str(row.get("runtime_id", "") or "").strip() or "unknown-runtime"
            count = int(row.get("count", 0) or 0)
            latest_reason = str(row.get("latest_reason", "") or "").strip() or "unknown"
            latest_caller = str(row.get("latest_caller", "") or "").strip()
            extra = f" latest_reason={latest_reason}"
            if latest_caller:
                extra += f" caller={latest_caller}"
            lines.append(f"- {actor} {runtime_id}: count={count}{extra}")

    noise_cost_summary = dict(summary.get("noise_cost_summary", {}) or {})
    if noise_cost_summary:
        assessment = str(noise_cost_summary.get("assessment", "") or "").strip()
        viewer_count = int(noise_cost_summary.get("viewer_reset_perf_count", 0) or 0)
        viewer_avg_ms = float(noise_cost_summary.get("viewer_reset_perf_avg_ms", 0.0) or 0.0)
        viewer_max_ms = float(noise_cost_summary.get("viewer_reset_perf_max_ms", 0.0) or 0.0)
        sender_count = int(noise_cost_summary.get("sender_noise_perf_count", 0) or 0)
        sender_avg_ms = float(noise_cost_summary.get("sender_noise_perf_avg_ms", 0.0) or 0.0)
        sender_max_ms = float(noise_cost_summary.get("sender_noise_perf_max_ms", 0.0) or 0.0)
        lines.append("Noise Cost:")
        lines.append(
            f"- assessment={assessment or 'unknown'} "
            f"viewer_reset_perf=count={viewer_count} avg_ms={viewer_avg_ms:.2f} max_ms={viewer_max_ms:.2f} "
            f"sender_noise_perf=count={sender_count} avg_ms={sender_avg_ms:.2f} max_ms={sender_max_ms:.2f}"
        )

    return "\n".join(lines)


def _bundle_dir_path() -> Path:
    bundle_dir = getattr(harness, "BUNDLE_DIR", None)
    if bundle_dir is None:
        return Path(".codex_tmp") / "drop_tracker_live_test_bundles"
    try:
        return Path(bundle_dir)
    except (TypeError, ValueError):
        return Path(".codex_tmp") / "drop_tracker_live_test_bundles"


def _extract_focus_event_ids(summary: dict[str, Any]) -> list[str]:
    event_ids: list[str] = []
    candidate_lists = [
        list(summary.get("lifecycle_gaps", [])),
        list(summary.get("accepted_missing_stats_binding", [])),
        list(summary.get("send_failed_events", [])),
        list(summary.get("stats_name_mismatches", [])),
        list(summary.get("suspicious_name_updates", [])),
        list(summary.get("invalid_target_events", [])),
        list(summary.get("forbidden_rows", [])),
        list(summary.get("problem_lifecycle_rows", [])),
    ]
    for rows in candidate_lists:
        for row in list(rows or []):
            if not isinstance(row, dict):
                continue
            event_id = str(row.get("event_id", "") or "").strip()
            if event_id and event_id not in event_ids:
                event_ids.append(event_id)
    return event_ids[:200]


def _write_bug_bundle_if_failed(
    *,
    summary: dict[str, Any],
    state: dict[str, Any],
    new_drop_rows: list[dict[str, Any]],
    new_debug_rows: list[dict[str, Any]],
    policy: dict[str, Any],
) -> str:
    return reporting.write_bug_bundle_if_failed(
        summary=summary,
        state=state,
        new_drop_rows=new_drop_rows,
        new_debug_rows=new_debug_rows,
        policy=policy,
        bundle_dir=_bundle_dir_path(),
        assess_fn=assess_summary,
        extract_focus_event_ids_fn=_extract_focus_event_ids,
    )


def _end() -> int:
    try:
        state = harness._read_state()
    except SystemExit:
        # Auto-arm first run so "done" works without a manual arm step.
        harness._refresh_baseline()
        state = harness._read_state()
    drop_rows = harness._load_csv_rows(harness.DROP_LOG_PATH)
    debug_rows = harness._load_jsonl_rows(harness.LIVE_DEBUG_PATH)

    slice_rows_fn = getattr(harness, "_slice_rows_since_baseline", None)
    if callable(slice_rows_fn):
        new_drop_rows, new_debug_rows = slice_rows_fn(state, drop_rows, debug_rows)
    else:
        drop_start = max(0, int(state.get("drop_row_count", 0)))
        debug_start = max(0, int(state.get("debug_row_count", 0)))
        new_drop_rows = drop_rows[drop_start:]
        new_debug_rows = debug_rows[debug_start:]
    policy = _load_oracle_policy()
    summary = harness._summarize(new_drop_rows, new_debug_rows)
    bug_bundle_path = _write_bug_bundle_if_failed(
        summary=summary,
        state=state,
        new_drop_rows=new_drop_rows,
        new_debug_rows=new_debug_rows,
        policy=policy,
    )
    if bug_bundle_path:
        summary["bug_bundle_path"] = bug_bundle_path
    print(format_report(summary))
    if bug_bundle_path:
        print(f"Bug Bundle: {bug_bundle_path}")
    print()
    print(json.dumps(summary, indent=2))
    passed, _failures, _warnings = assess_summary(summary, policy)
    # Keep windows clean by default: every `done` becomes baseline for the next run.
    harness._refresh_baseline()
    return 0 if passed else 1


def _baseline_exists() -> bool:
    state_path = getattr(harness, "STATE_PATH", None)
    if state_path is None:
        return False
    try:
        return bool(state_path.exists())
    except OSError:
        return False


def _auto() -> int:
    if not _baseline_exists():
        begin_rc = harness._begin()
        print("AUTO MODE: baseline armed. Run your test session, then run auto/done again.")
        return int(begin_rc)

    state = harness._read_state()
    drop_rows = harness._load_csv_rows(harness.DROP_LOG_PATH)
    debug_rows = harness._load_jsonl_rows(harness.LIVE_DEBUG_PATH)

    slice_rows_fn = getattr(harness, "_slice_rows_since_baseline", None)
    if callable(slice_rows_fn):
        new_drop_rows, new_debug_rows = slice_rows_fn(state, drop_rows, debug_rows)
    else:
        drop_start = max(0, int(state.get("drop_row_count", 0)))
        debug_start = max(0, int(state.get("debug_row_count", 0)))
        new_drop_rows = drop_rows[drop_start:]
        new_debug_rows = debug_rows[debug_start:]

    if not new_drop_rows and not new_debug_rows:
        print("AUTO MODE: no new tracker/debug rows since baseline.")
        return 0

    policy = _load_oracle_policy()
    summary = harness._summarize(new_drop_rows, new_debug_rows)
    bug_bundle_path = _write_bug_bundle_if_failed(
        summary=summary,
        state=state,
        new_drop_rows=new_drop_rows,
        new_debug_rows=new_debug_rows,
        policy=policy,
    )
    if bug_bundle_path:
        summary["bug_bundle_path"] = bug_bundle_path
    print(format_report(summary))
    if bug_bundle_path:
        print(f"Bug Bundle: {bug_bundle_path}")
    print()
    print(json.dumps(summary, indent=2))
    passed, _failures, _warnings = assess_summary(summary, policy)
    harness._refresh_baseline()
    return 0 if passed else 1


def main(argv: list[str]) -> int:
    command = argv[1].strip().lower() if len(argv) > 1 else "status"
    if command in {"begin", "arm", "start"}:
        return harness._begin()
    if command in {"status", "check"}:
        return harness._status()
    if command in {"end", "finish", "done"}:
        return _end()
    if command in {"auto", "watch"}:
        return _auto()
    raise SystemExit(
        "Usage: python drop_tracker_live_test_report.py "
        "[status|check|begin|arm|start|end|finish|done|auto|watch]"
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
