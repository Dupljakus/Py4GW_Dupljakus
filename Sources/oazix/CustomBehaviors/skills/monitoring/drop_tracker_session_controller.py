import time

from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_sender_state import begin_new_session

RESET_COALESCE_WINDOW_S = 8.0
LOW_UPTIME_RESET_MAX_MS = 5000


def is_same_low_uptime_reset_window(
    current_map_id: int,
    last_reset_map_id: int,
    current_uptime_ms: int,
    last_reset_uptime_ms: int,
    now_ts: float,
    last_reset_started_at: float,
) -> bool:
    return (
        current_map_id > 0
        and current_map_id == last_reset_map_id
        and (now_ts - last_reset_started_at) <= RESET_COALESCE_WINDOW_S
        and current_uptime_ms > 0
        and last_reset_uptime_ms > 0
        and current_uptime_ms <= LOW_UPTIME_RESET_MAX_MS
        and last_reset_uptime_ms <= LOW_UPTIME_RESET_MAX_MS
    )


def begin_sender_tracking_session(sender, reason: str, current_map_id: int = 0, current_instance_uptime_ms: int = 0) -> None:
    normalized_reason = str(reason or "").strip() or "unknown"
    normalized_map_id = int(current_map_id or 0)
    normalized_uptime_ms = int(current_instance_uptime_ms or 0)
    now_ts = time.time()
    last_reset_reason = str(getattr(sender, "last_reset_reason", "") or "").strip()
    last_reset_map_id = int(getattr(sender, "last_reset_map_id", 0) or 0)
    last_reset_uptime_ms = int(getattr(sender, "last_reset_instance_uptime_ms", 0) or 0)
    last_reset_started_at = float(getattr(sender, "last_reset_started_at", 0.0) or 0.0)
    reset_origin = str(getattr(sender, "pending_reset_origin", "") or "").strip()
    reset_source_runtime_id = str(getattr(sender, "pending_reset_source_runtime_id", "") or "").strip()
    reset_source_caller = str(getattr(sender, "pending_reset_source_caller", "") or "").strip()
    reset_source_sequence = int(getattr(sender, "pending_reset_source_sequence", 0) or 0)
    sender.pending_reset_origin = ""
    sender.pending_reset_source_runtime_id = ""
    sender.pending_reset_source_caller = ""
    sender.pending_reset_source_sequence = 0
    same_low_uptime_window = is_same_low_uptime_reset_window(
        normalized_map_id,
        last_reset_map_id,
        normalized_uptime_ms,
        last_reset_uptime_ms,
        now_ts,
        last_reset_started_at,
    )
    duplicate_reset = (
        normalized_map_id > 0
        and normalized_map_id == last_reset_map_id
        and (now_ts - last_reset_started_at) <= RESET_COALESCE_WINDOW_S
        and normalized_uptime_ms > 0
        and last_reset_uptime_ms > 0
        and (
            same_low_uptime_window
            or (
                normalized_reason == last_reset_reason
                and abs(normalized_uptime_ms - last_reset_uptime_ms) <= 2500
            )
        )
    )
    if duplicate_reset:
        if same_low_uptime_window:
            sender.last_reset_reason = normalized_reason
        sender.last_seen_map_id = normalized_map_id
        sender.last_seen_instance_uptime_ms = max(last_reset_uptime_ms, normalized_uptime_ms)
        sender.last_reset_instance_uptime_ms = max(last_reset_uptime_ms, normalized_uptime_ms)
        return
    existing_carryover_snapshot = (
        dict(sender.carryover_inventory_snapshot)
        if getattr(sender, "carryover_inventory_snapshot", None)
        else {}
    )
    previous_carryover_suppression_until = float(getattr(sender, "carryover_suppression_until", 0.0) or 0.0)
    current_snapshot = dict(sender.last_inventory_snapshot) if sender.last_inventory_snapshot else {}
    live_snapshot = {}
    take_inventory_snapshot = getattr(sender, "_take_inventory_snapshot", None)
    if (
        not current_snapshot
        and not existing_carryover_snapshot
        and normalized_map_id > 0
        and callable(take_inventory_snapshot)
    ):
        try:
            live_snapshot = dict(take_inventory_snapshot() or {})
        except (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError, OSError):
            live_snapshot = {}
    if not current_snapshot and live_snapshot:
        current_snapshot = live_snapshot
    if existing_carryover_snapshot and current_snapshot:
        carryover_snapshot = (
            current_snapshot
            if len(current_snapshot) >= len(existing_carryover_snapshot)
            else existing_carryover_snapshot
        )
    else:
        carryover_snapshot = current_snapshot or existing_carryover_snapshot
    begin_new_session(
        sender,
        normalized_reason,
        current_map_id=normalized_map_id,
        current_instance_uptime_ms=normalized_uptime_ms,
    )
    sender.last_reset_reason = normalized_reason
    sender.last_reset_map_id = normalized_map_id
    sender.last_reset_instance_uptime_ms = normalized_uptime_ms
    sender.last_reset_started_at = now_ts
    sender.carryover_inventory_snapshot = carryover_snapshot
    sender.session_startup_pending = bool(carryover_snapshot)
    sender.startup_stable_snapshot_credit = (
        1
        if bool(carryover_snapshot) and normalized_reason in {"instance_change", "viewer_sync_reset"}
        else 0
    )
    grace_seconds = max(6.0, float(getattr(sender, "warmup_grace_seconds", 3.0) or 3.0) + 9.0)
    next_carryover_suppression_until = time.time() + grace_seconds if carryover_snapshot else 0.0
    sender.carryover_suppression_until = max(
        previous_carryover_suppression_until,
        next_carryover_suppression_until,
    )
    sender._append_live_debug_log(
        "sender_session_reset",
        f"transition={str(reason or '').strip() or 'unknown'}",
        reason=str(reason or "").strip() or "unknown",
        current_map_id=int(current_map_id or 0),
        current_instance_uptime_ms=int(current_instance_uptime_ms or 0),
        sender_session_id=int(getattr(sender, "sender_session_id", 0) or 0),
        sender_runtime_id=str(getattr(sender, "sender_runtime_id", "") or ""),
        sender_runtime_generation=int(getattr(sender, "sender_runtime_generation", 0) or 0),
        sender_tick_sequence=int(getattr(sender, "sender_tick_sequence", 0) or 0),
        reset_origin=reset_origin,
        reset_source_runtime_id=reset_source_runtime_id,
        reset_source_caller=reset_source_caller,
        reset_source_sequence=reset_source_sequence,
        carryover_count=len(carryover_snapshot),
        startup_pending=bool(sender.session_startup_pending),
        startup_stable_snapshot_credit=int(getattr(sender, "startup_stable_snapshot_credit", 0) or 0),
        carryover_suppression_until=float(getattr(sender, "carryover_suppression_until", 0.0) or 0.0),
    )
