import csv
import os
import time

from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_log_store import (
    DROP_LOG_HEADER,
    parse_drop_log_file,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_runtime_store import (
    build_state_from_parsed_rows,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_viewer_runtime_context import (
    EXPECTED_RUNTIME_ERRORS,
    Map,
    Py4GW,
    runtime_attr,
)


def reset_live_log_file(viewer, truncate: bool = True):
    os_module = runtime_attr(viewer, "os", os)
    csv_module = runtime_attr(viewer, "csv", csv)
    time_module = runtime_attr(viewer, "time", time)
    py4gw_module = runtime_attr(viewer, "Py4GW", Py4GW)
    header = runtime_attr(viewer, "DROP_LOG_HEADER", DROP_LOG_HEADER)
    try:
        log_path = str(getattr(viewer, "log_path", "") or "").strip()
        if not log_path:
            return
        os_module.makedirs(os_module.path.dirname(log_path), exist_ok=True)
        if bool(truncate) or not os_module.path.isfile(log_path):
            with open(log_path, mode="w", newline="", encoding="utf-8") as f:
                writer = csv_module.writer(f)
                writer.writerow(list(header))
        viewer.last_read_time = os_module.path.getmtime(log_path) if os_module.path.isfile(log_path) else time_module.time()
    except EXPECTED_RUNTIME_ERRORS as e:
        py4gw_module.Console.Log("DropViewer", f"Failed to reset live log file: {e}", py4gw_module.Console.MessageType.Warning)


def load_drops(viewer):
    os_module = runtime_attr(viewer, "os", os)
    if not os_module.path.isfile(viewer.log_path):
        viewer.raw_drops = []
        viewer.aggregated_drops = {}
        viewer.total_drops = 0
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
        mark_rows_changed = getattr(viewer, "_mark_rows_changed", None)
        if callable(mark_rows_changed):
            mark_rows_changed("load_drops_missing_log")
        return

    try:
        current_mtime = os_module.path.getmtime(viewer.log_path)
        if current_mtime <= viewer.last_read_time:
            return
        viewer.last_read_time = current_mtime
        viewer._parse_log_file(viewer.log_path)
    except EXPECTED_RUNTIME_ERRORS as e:
        viewer.set_status(f"Error reading log: {e}")


def parse_log_file(viewer, filepath):
    parse_drop_log_file_fn = runtime_attr(viewer, "parse_drop_log_file", parse_drop_log_file)
    build_state_fn = runtime_attr(viewer, "build_state_from_parsed_rows", build_state_from_parsed_rows)
    map_module = runtime_attr(viewer, "Map", Map)
    parsed_rows = parse_drop_log_file_fn(filepath, map_name_resolver=map_module.GetMapName)
    floor_row_count = max(0, viewer._safe_int(getattr(viewer, "live_session_log_floor_row_count", 0), 0))
    if floor_row_count > 0:
        if len(parsed_rows) <= floor_row_count:
            parsed_rows = []
        else:
            parsed_rows = parsed_rows[floor_row_count:]
    temp_drops, temp_agg, total, temp_stats_by_event = build_state_fn(
        parsed_rows=parsed_rows,
        ensure_text_fn=viewer._ensure_text,
        make_stats_cache_key_fn=viewer._make_stats_cache_key,
        canonical_name_fn=viewer._canonical_agg_item_name,
    )
    viewer.raw_drops = temp_drops
    viewer.aggregated_drops = temp_agg
    viewer.total_drops = int(total)
    viewer.name_chunk_buffers = {}
    viewer.full_name_by_signature = {}
    viewer.stats_by_event = temp_stats_by_event
    viewer.stats_chunk_buffers = {}
    viewer.stats_payload_by_event = {}
    viewer.stats_payload_chunk_buffers = {}
    viewer.event_state_by_key = {}
    viewer.stats_render_cache_by_event = {}
    viewer.stats_name_signature_by_event = {}
    viewer.remote_stats_request_last_by_event = {}
    viewer.remote_stats_pending_by_event = {}
    viewer._log_autoscroll_initialized = False
    viewer._last_log_autoscroll_total_drops = int(viewer.total_drops)
    mark_rows_changed = getattr(viewer, "_mark_rows_changed", None)
    if callable(mark_rows_changed):
        mark_rows_changed("parse_log_file")


def parse_log_file_local(viewer, *args, **kwargs):
    return parse_log_file(viewer, *args, **kwargs)
