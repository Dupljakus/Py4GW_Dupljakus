import time

from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_utility import (
    DropTrackerSender,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_viewer_runtime_context import (
    EXPECTED_RUNTIME_ERRORS,
    GLOBAL_CACHE,
    Player,
    Py4GW,
    runtime_attr,
)


def reset_live_session(viewer, preserve_live_log: bool = False):
    parse_drop_log_file_fn = runtime_attr(viewer, "parse_drop_log_file", None)
    map_module = runtime_attr(viewer, "Map", None)
    os_module = runtime_attr(viewer, "os", None)
    viewer._seal_sender_session_floors()
    preserved_row_floor = 0
    if bool(preserve_live_log) and callable(parse_drop_log_file_fn) and map_module is not None and os_module is not None:
        try:
            log_path = str(getattr(viewer, "log_path", "") or "").strip()
            if log_path and os_module.path.isfile(log_path):
                preserved_row_floor = len(parse_drop_log_file_fn(log_path, map_name_resolver=map_module.GetMapName))
        except EXPECTED_RUNTIME_ERRORS:
            preserved_row_floor = max(0, viewer._safe_int(getattr(viewer, "live_session_log_floor_row_count", 0), 0))
    viewer.live_session_log_floor_row_count = preserved_row_floor if bool(preserve_live_log) else 0
    viewer.raw_drops = []
    viewer.aggregated_drops = {}
    viewer.total_drops = 0
    viewer.selected_item_key = None
    viewer.selected_log_row = None
    viewer._log_autoscroll_initialized = False
    viewer._last_log_autoscroll_total_drops = 0
    viewer._request_log_scroll_bottom = False
    viewer._request_agg_scroll_bottom = False
    viewer.shmem_bootstrap_done = False
    viewer.last_read_time = 0
    viewer.recent_log_cache = {}
    viewer.seen_events = {}
    viewer.name_chunk_buffers = {}
    viewer.full_name_by_signature = {}
    viewer.stats_by_event = {}
    viewer.stats_chunk_buffers = {}
    viewer.stats_payload_by_event = {}
    viewer.stats_payload_chunk_buffers = {}
    viewer.event_state_by_key = {}
    viewer.stats_render_cache_by_event = {}
    viewer.stats_name_signature_by_event = {}
    viewer.remote_stats_request_last_by_event = {}
    viewer.remote_stats_pending_by_event = {}
    viewer.auto_id_selected_stats_refresh_last_ts = 0.0
    viewer.auto_id_background_stats_refresh_last_ts = 0.0
    viewer.auto_id_background_stats_scan_cursor = 0
    viewer.model_name_by_id = {}
    viewer.name_trace_recent_lines = []
    viewer._shmem_scan_start_index = 0
    viewer.identify_response_scheduler.clear()
    viewer.pending_identify_mod_capture = {}
    viewer._reset_live_log_file(truncate=not bool(preserve_live_log))
    mark_rows_changed = getattr(viewer, "_mark_rows_changed", None)
    if callable(mark_rows_changed):
        mark_rows_changed("reset_live_session")


def arm_reset_trace(
    viewer,
    reason: str,
    previous_map_id: int = 0,
    current_map_id: int = 0,
    previous_instance_uptime_ms: int = 0,
    current_instance_uptime_ms: int = 0,
):
    time_module = runtime_attr(viewer, "time", time)
    viewer.reset_trace_until = time_module.time() + 20.0
    viewer.reset_trace_drop_logs_remaining = 18
    viewer._log_reset_trace(
        (
            f"RESET TRACE armed reason={str(reason or 'unknown')} "
            f"actor={viewer._reset_trace_actor_label()} "
            f"map={int(previous_map_id or 0)}->{int(current_map_id or 0)} "
            f"uptime_ms={int(previous_instance_uptime_ms or 0)}->{int(current_instance_uptime_ms or 0)} "
            f"rows={len(viewer.raw_drops)} total={int(viewer.total_drops)}"
        )
    )


def reset_trace_active(viewer) -> bool:
    time_module = runtime_attr(viewer, "time", time)
    return time_module.time() <= float(getattr(viewer, "reset_trace_until", 0.0) or 0.0)


def reset_trace_actor_label(viewer) -> str:
    player_module = runtime_attr(viewer, "Player", Player)
    try:
        actor_name = viewer._ensure_text(player_module.GetName()).strip()
    except EXPECTED_RUNTIME_ERRORS:
        actor_name = ""
    try:
        actor_email = viewer._ensure_text(player_module.GetAccountEmail()).strip()
    except EXPECTED_RUNTIME_ERRORS:
        actor_email = ""
    actor_name = actor_name or "Unknown"
    actor_email = actor_email or "unknown@email"
    return f"{actor_name}<{actor_email}>"


def log_reset_trace(viewer, message: str, consume: bool = False):
    py4gw_module = runtime_attr(viewer, "Py4GW", Py4GW)
    if not viewer._reset_trace_active():
        return
    if consume:
        remaining = int(getattr(viewer, "reset_trace_drop_logs_remaining", 0) or 0)
        if remaining <= 0:
            return
        viewer.reset_trace_drop_logs_remaining = remaining - 1
    viewer.reset_trace_lines.append(str(message or ""))
    if len(viewer.reset_trace_lines) > 120:
        del viewer.reset_trace_lines[:-120]
    py4gw_module.Console.Log(
        "DropViewer",
        str(message or ""),
        py4gw_module.Console.MessageType.Warning,
    )


def get_reset_trace_lines(viewer) -> list[str]:
    sender_cls = runtime_attr(viewer, "DropTrackerSender", DropTrackerSender)
    lines = []
    viewer_lines = getattr(viewer, "reset_trace_lines", None)
    if isinstance(viewer_lines, list):
        for line in viewer_lines:
            txt = viewer._ensure_text(line).strip()
            if txt:
                lines.append(f"Viewer | {txt}")
    try:
        sender = sender_cls()
        sender_lines = getattr(sender, "debug_reset_trace_lines", None)
        if isinstance(sender_lines, list):
            for line in sender_lines:
                txt = viewer._ensure_text(line).strip()
                if txt:
                    lines.append(f"Sender | {txt}")
    except EXPECTED_RUNTIME_ERRORS:
        pass
    return lines[-160:]


def clear_reset_trace_lines(viewer):
    sender_cls = runtime_attr(viewer, "DropTrackerSender", DropTrackerSender)
    viewer.reset_trace_lines = []
    try:
        sender = sender_cls()
        if isinstance(getattr(sender, "debug_reset_trace_lines", None), list):
            sender.debug_reset_trace_lines = []
    except EXPECTED_RUNTIME_ERRORS:
        pass


def log_map_watch(viewer, message: str):
    py4gw_module = runtime_attr(viewer, "Py4GW", Py4GW)
    msg = viewer._ensure_text(message).strip()
    if not msg:
        return
    viewer.map_watch_lines.append(msg)
    if len(viewer.map_watch_lines) > 120:
        del viewer.map_watch_lines[:-120]
    py4gw_module.Console.Log("DropViewer", msg, py4gw_module.Console.MessageType.Warning)


def get_map_watch_lines(viewer) -> list[str]:
    return list(getattr(viewer, "map_watch_lines", []) or [])[-120:]


def clear_map_watch_lines(viewer):
    viewer.map_watch_lines = []


def seal_sender_session_floors(viewer):
    floors = getattr(viewer, "sender_session_floor_by_email", None)
    if not isinstance(floors, dict):
        viewer.sender_session_floor_by_email = {}
        floors = viewer.sender_session_floor_by_email
    last_seen = getattr(viewer, "sender_session_last_seen_by_email", None)
    if not isinstance(last_seen, dict):
        return
    for sender_email, session_id in list(last_seen.items()):
        sender_key = viewer._ensure_text(sender_email).strip().lower()
        session_value = max(0, viewer._safe_int(session_id, 0))
        if not sender_key or session_value <= 0:
            continue
        floors[sender_key] = max(max(0, viewer._safe_int(floors.get(sender_key, 0), 0)), session_value)


def reset_sender_tracking_session(viewer, current_map_id: int = 0, current_instance_uptime_ms: int = 0):
    sender_cls = runtime_attr(viewer, "DropTrackerSender", DropTrackerSender)
    try:
        sender = sender_cls()
        if sender is None:
            return
        if hasattr(sender, "_begin_new_session"):
            sender.pending_reset_origin = "viewer_sync_reset"
            sender.pending_reset_source_runtime_id = str(getattr(viewer, "viewer_runtime_id", "") or "")
            sender.pending_reset_source_caller = str(getattr(viewer, "last_update_caller", "") or "")
            sender.pending_reset_source_sequence = int(getattr(viewer, "last_update_sequence", 0) or 0)
            sender._begin_new_session("viewer_sync_reset", current_map_id, current_instance_uptime_ms)
        else:
            try:
                sender._arm_reset_trace("viewer_sync_reset", current_map_id, current_instance_uptime_ms)
            except EXPECTED_RUNTIME_ERRORS:
                pass
            sender._reset_tracking_state()
            sender.last_seen_map_id = max(0, viewer._safe_int(current_map_id, 0))
            sender.last_seen_instance_uptime_ms = max(0, viewer._safe_int(current_instance_uptime_ms, 0))
    except EXPECTED_RUNTIME_ERRORS:
        return


def drain_pending_tracker_messages(viewer, max_passes: int = 6) -> int:
    player_module = runtime_attr(viewer, "Player", Player)
    global_cache = runtime_attr(viewer, "GLOBAL_CACHE", GLOBAL_CACHE)
    passes = 0
    try:
        my_email = viewer._ensure_text(player_module.GetAccountEmail()).strip()
        if not my_email:
            return 0
        shmem = getattr(global_cache, "ShMem", None)
        if shmem is None:
            return 0
        tracker_tags = {
            "TrackerDrop",
            "TrackerNameV2",
            "TrackerStatsV1",
            "TrackerStatsV2",
            "TrackerAckV2",
        }
        max_pass_count = max(1, viewer._safe_int(max_passes, 6))
        for _pass_idx in range(max_pass_count):
            before_pending = 0
            for _msg_idx, shared_msg in shmem.GetAllMessages():
                receiver_email = viewer._ensure_text(getattr(shared_msg, "ReceiverEmail", "")).strip()
                if receiver_email != my_email:
                    continue
                extra_data_list = getattr(shared_msg, "ExtraData", None)
                if not extra_data_list or len(extra_data_list) <= 0:
                    continue
                tag = viewer._ensure_text(extra_data_list[0]).strip()
                if tag in tracker_tags:
                    before_pending += 1
            if before_pending <= 0:
                break
            viewer._poll_shared_memory()
            passes += 1
            after_pending = 0
            for _msg_idx, shared_msg in shmem.GetAllMessages():
                receiver_email = viewer._ensure_text(getattr(shared_msg, "ReceiverEmail", "")).strip()
                if receiver_email != my_email:
                    continue
                extra_data_list = getattr(shared_msg, "ExtraData", None)
                if not extra_data_list or len(extra_data_list) <= 0:
                    continue
                tag = viewer._ensure_text(extra_data_list[0]).strip()
                if tag in tracker_tags:
                    after_pending += 1
            if after_pending <= 0 or after_pending >= before_pending:
                break
    except EXPECTED_RUNTIME_ERRORS:
        return passes
    return passes


def begin_new_explorable_session(
    viewer,
    reason: str,
    current_map_id: int = 0,
    current_instance_uptime_ms: int = 0,
    status_message: str = "Auto reset on map change",
) -> None:
    player_module = runtime_attr(viewer, "Player", Player)
    time_module = runtime_attr(viewer, "time", time)
    now_ts = time_module.time()
    reset_started_perf = time_module.perf_counter()
    normalized_map_id = max(0, viewer._safe_int(current_map_id, 0))
    normalized_uptime_ms = max(0, viewer._safe_int(current_instance_uptime_ms, 0))
    normalized_reason = str(reason or "").strip() or "unknown"
    duplicate_reset_window_s = 8.0
    last_session_reset_map_id = max(0, viewer._safe_int(getattr(viewer, "last_session_reset_map_id", 0), 0))
    last_session_reset_started_at = float(getattr(viewer, "last_session_reset_started_at", 0.0) or 0.0)
    last_session_reset_uptime_ms = max(
        0,
        viewer._safe_int(getattr(viewer, "last_session_reset_instance_uptime_ms", 0), 0),
    )
    last_session_reset_reason = str(getattr(viewer, "last_session_reset_reason", "") or "").strip()
    same_map_recent_reset = bool(
        normalized_map_id > 0
        and normalized_map_id == last_session_reset_map_id
        and (now_ts - last_session_reset_started_at) <= duplicate_reset_window_s
    )
    same_reason_close_uptime = bool(
        normalized_reason == last_session_reset_reason
        and normalized_uptime_ms > 0
        and last_session_reset_uptime_ms > 0
        and abs(normalized_uptime_ms - last_session_reset_uptime_ms) <= 2500
    )
    preserve_current_session = bool(
        same_map_recent_reset
        and same_reason_close_uptime
    )
    viewer._arm_reset_trace(
        reason,
        viewer.last_seen_map_id,
        normalized_map_id,
        viewer.last_seen_instance_uptime_ms,
        normalized_uptime_ms,
    )
    viewer.last_seen_map_id = normalized_map_id
    viewer.last_seen_instance_uptime_ms = normalized_uptime_ms
    if preserve_current_session:
        viewer.last_session_reset_reason = normalized_reason
        viewer.last_session_reset_instance_uptime_ms = max(
            last_session_reset_uptime_ms,
            normalized_uptime_ms,
        )
        viewer._log_reset_trace(
            (
                f"RESET TRACE preserved actor={viewer._reset_trace_actor_label()} "
                f"reason={str(reason or 'unknown')} map={normalized_map_id} "
                f"uptime_ms={normalized_uptime_ms} rows={len(viewer.raw_drops)} total={int(viewer.total_drops)}"
            ),
            consume=True,
        )
        viewer.set_status(status_message)
        return
    viewer.last_session_reset_map_id = normalized_map_id
    viewer.last_session_reset_started_at = now_ts
    viewer.last_session_reset_reason = normalized_reason
    viewer.last_session_reset_instance_uptime_ms = normalized_uptime_ms
    drained_passes = viewer._drain_pending_tracker_messages()
    if drained_passes > 0:
        viewer._log_reset_trace(
            (
                f"RESET TRACE drained actor={viewer._reset_trace_actor_label()} "
                f"passes={int(drained_passes)} reason={str(reason or 'unknown')} "
                f"map={normalized_map_id} uptime_ms={normalized_uptime_ms}"
            ),
            consume=True,
        )
    viewer._reset_sender_tracking_session(normalized_map_id, normalized_uptime_ms)
    viewer._reset_live_session(preserve_live_log=True)
    flushed_messages = viewer._flush_pending_tracker_messages()
    viewer.map_change_ignore_until = time_module.time() + 3.0
    viewer._arm_chat_history_catchup()
    viewer.set_status(status_message)
    append_live_debug_log = getattr(viewer, "_append_live_debug_log", None)
    if callable(append_live_debug_log):
        append_live_debug_log(
            "viewer_reset_perf",
            f"duration_ms={(time_module.perf_counter() - reset_started_perf) * 1000.0:.2f}",
            reason=str(reason or "unknown"),
            current_map_id=normalized_map_id,
            current_instance_uptime_ms=normalized_uptime_ms,
            duration_ms=round((time_module.perf_counter() - reset_started_perf) * 1000.0, 3),
            drained_passes=int(drained_passes),
            flushed_messages=int(flushed_messages),
        )


def flush_pending_tracker_messages(viewer) -> int:
    player_module = runtime_attr(viewer, "Player", Player)
    global_cache = runtime_attr(viewer, "GLOBAL_CACHE", GLOBAL_CACHE)
    flushed = 0
    try:
        my_email = viewer._ensure_text(player_module.GetAccountEmail()).strip()
        if not my_email:
            return 0
        shmem = getattr(global_cache, "ShMem", None)
        if shmem is None:
            return 0
        tracker_tags = {
            "TrackerDrop",
            "TrackerNameV2",
            "TrackerStatsV1",
            "TrackerStatsV2",
            "TrackerAckV2",
        }
        for msg_idx, shared_msg in shmem.GetAllMessages():
            receiver_email = viewer._ensure_text(getattr(shared_msg, "ReceiverEmail", "")).strip()
            if receiver_email != my_email:
                continue
            extra_data_list = getattr(shared_msg, "ExtraData", None)
            if not extra_data_list or len(extra_data_list) <= 0:
                continue
            tag = viewer._ensure_text(extra_data_list[0]).strip()
            if tag not in tracker_tags:
                continue
            shmem.MarkMessageAsFinished(my_email, msg_idx)
            flushed += 1
    except EXPECTED_RUNTIME_ERRORS:
        return flushed
    return flushed
