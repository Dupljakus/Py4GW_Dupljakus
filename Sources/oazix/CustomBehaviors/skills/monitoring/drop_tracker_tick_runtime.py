import sys
import time

from Py4GWCoreLib import Py4GW, Routines

from Sources.oazix.CustomBehaviors.primitives.helpers.map_instance_helper import (
    classify_map_instance_transition,
    read_current_map_instance,
)

EXPECTED_RUNTIME_ERRORS = (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError, OSError)


def _sender_runtime_attr(sender, name: str, fallback):
    try:
        module = sys.modules.get(sender.__class__.__module__)
    except EXPECTED_RUNTIME_ERRORS:
        module = None
    if module is not None and hasattr(module, name):
        return getattr(module, name)
    return fallback


def run_sender_tick(sender) -> None:
    if not sender.enabled:
        return
    try:
        if not Routines.Checks.Map.MapValid():
            sender._begin_new_session("map_invalid", 0, 0)
            return
        read_current_map_instance_fn = _sender_runtime_attr(sender, "read_current_map_instance", read_current_map_instance)
        classify_transition_fn = _sender_runtime_attr(
            sender,
            "classify_map_instance_transition",
            classify_map_instance_transition,
        )
        current_map_id, current_instance_uptime_ms = read_current_map_instance_fn()
        if current_map_id > 0:
            if int(sender.last_seen_map_id) <= 0:
                sender.last_seen_map_id = current_map_id
                sender.last_seen_instance_uptime_ms = current_instance_uptime_ms
            else:
                transition_reason = classify_transition_fn(
                    previous_map_id=sender.last_seen_map_id,
                    previous_instance_uptime_ms=sender.last_seen_instance_uptime_ms,
                    current_map_id=current_map_id,
                    current_instance_uptime_ms=current_instance_uptime_ms,
                )
                if transition_reason:
                    sender.pending_reset_origin = "sender_tick"
                    sender.pending_reset_source_runtime_id = str(getattr(sender, "sender_runtime_id", "") or "")
                    sender.pending_reset_source_caller = "run_sender_tick"
                    sender.pending_reset_source_sequence = int(getattr(sender, "sender_tick_sequence", 0) or 0)
                    sender._begin_new_session(transition_reason, current_map_id, current_instance_uptime_ms)
                    if (
                        transition_reason in {"instance_change", "viewer_sync_reset"}
                        and bool(getattr(sender, "session_startup_pending", False))
                        and bool(getattr(sender, "carryover_inventory_snapshot", {}))
                    ):
                        sender._append_live_debug_log(
                            "sender_post_reset_scan_scheduled",
                            f"transition={transition_reason}",
                            transition_reason=str(transition_reason or "").strip(),
                            carryover_count=len(getattr(sender, "carryover_inventory_snapshot", {}) or {}),
                            current_map_id=int(current_map_id or 0),
                            current_instance_uptime_ms=int(current_instance_uptime_ms or 0),
                        )
                        sender._process_inventory_deltas()
                        if sender.outbox_queue:
                            sender._flush_outbox()
                        if sender.pending_name_refresh_by_event:
                            sender._process_pending_name_refreshes()
                    return
            sender.last_seen_instance_uptime_ms = current_instance_uptime_ms
        sender.sender_tick_sequence = int(getattr(sender, "sender_tick_sequence", 0) or 0) + 1
        now_ts = time.time()
        heartbeat_interval_s = max(1.0, float(getattr(sender, "heartbeat_interval_s", 2.5) or 2.5))
        last_heartbeat_log_at = float(getattr(sender, "last_heartbeat_log_at", 0.0) or 0.0)
        if (now_ts - last_heartbeat_log_at) >= heartbeat_interval_s:
            sender.last_heartbeat_log_at = now_ts
            sender._append_live_debug_log(
                "sender_runtime_heartbeat",
                f"map={int(current_map_id or 0)} uptime_ms={int(current_instance_uptime_ms or 0)}",
                current_map_id=int(current_map_id or 0),
                current_instance_uptime_ms=int(current_instance_uptime_ms or 0),
                snapshot_size=len(getattr(sender, "last_inventory_snapshot", {}) or {}),
                pending_outbox_count=len(getattr(sender, "outbox_queue", []) or []),
                pending_name_refresh_count=len(getattr(sender, "pending_name_refresh_by_event", {}) or {}),
                log_path=str(getattr(sender, "runtime_config_path", "") or "").strip(),
                live_debug_log_path=str(getattr(sender, "live_debug_log_path", "") or "").strip(),
            )
        if sender.config_poll_timer.IsExpired():
            sender.config_poll_timer.Reset()
            sender._load_runtime_config()
        if sender.debug_enabled and sender.debug_timer.IsExpired():
            sender.debug_timer.Reset()
            Py4GW.Console.Log(
                "DropTrackerSender",
                (
                    "active LIVE "
                    f"snapshot_size={len(sender.last_inventory_snapshot)} "
                    f"items={sender.last_snapshot_total} "
                    f"ready={sender.last_snapshot_ready} "
                    f"not_ready={sender.last_snapshot_not_ready} "
                    f"sent={sender.last_sent_count} "
                    f"candidates={sender.last_candidate_count} "
                    f"enqueued={sender.last_enqueued_count} "
                    f"queued={len(sender.outbox_queue)} "
                    f"acks={sender.last_ack_count} "
                    f"pending_names={len(sender.pending_slot_deltas)} "
                    f"world_live={int(sender.last_world_item_scan_count)} "
                    f"world_recent={len(sender.recent_world_item_disappearances)} "
                    f"name_refresh={len(sender.pending_name_refresh_by_event)} "
                    f"role={'leader' if sender._is_party_leader_client() else 'follower'} "
                    f"warmed={sender.is_warmed_up} "
                    f"proc_ms={sender.last_process_duration_ms:.2f}"
                ),
                Py4GW.Console.MessageType.Info,
            )
        if sender.inventory_poll_timer.IsExpired():
            if sender.world_item_poll_timer.IsExpired():
                sender.world_item_poll_timer.Reset()
                sender._poll_world_item_disappearances()
            sender.inventory_poll_timer.Reset()
            sender._process_inventory_deltas()
        elif sender.world_item_poll_timer.IsExpired():
            sender.world_item_poll_timer.Reset()
            sender._poll_world_item_disappearances()
        if sender.outbox_queue:
            sender._flush_outbox()
        if sender.pending_name_refresh_by_event:
            sender._process_pending_name_refreshes()
    except EXPECTED_RUNTIME_ERRORS:
        return
