import os

from Py4GWCoreLib.Py4GWcorelib import ThrottledTimer

from Sources.oazix.CustomBehaviors.primitives import constants
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_live_debug import get_live_debug_log_path


def _set_if_missing(sender, attr_name: str, value) -> None:
    if not hasattr(sender, attr_name):
        setattr(sender, attr_name, value)


def sync_existing_sender_runtime_state(sender, state_version: int, sender_runtime_generation: int) -> None:
    _set_if_missing(sender, "pending_slot_deltas", {})
    _set_if_missing(sender, "outbox_queue", [])
    _set_if_missing(sender, "pending_name_refresh_by_event", {})
    _set_if_missing(sender, "carryover_inventory_snapshot", {})
    _set_if_missing(sender, "carryover_suppression_until", 0.0)
    _set_if_missing(sender, "stable_snapshot_count", 0)
    _set_if_missing(sender, "session_startup_pending", False)
    _set_if_missing(sender, "warmup_grace_seconds", 3.0)
    _set_if_missing(sender, "warmup_grace_until", 0.0)
    _set_if_missing(sender, "pending_ttl_seconds", 6.0)
    _set_if_missing(sender, "debug_pipeline_logs", False)
    _set_if_missing(sender, "live_debug_detailed", True)
    _set_if_missing(sender, "max_outbox_size", 2000)
    _set_if_missing(sender, "max_snapshot_size_jump", 40)
    _set_if_missing(sender, "last_known_is_leader", False)
    _set_if_missing(sender, "current_receiver_email", "")
    _set_if_missing(sender, "last_reset_reason", "")
    _set_if_missing(sender, "last_reset_map_id", 0)
    _set_if_missing(sender, "last_reset_instance_uptime_ms", 0)
    _set_if_missing(sender, "last_reset_started_at", 0.0)
    _set_if_missing(sender, "current_world_item_agents", {})
    _set_if_missing(sender, "recent_world_item_disappearances", [])
    _set_if_missing(sender, "world_item_seen_since_reset", False)
    _set_if_missing(sender, "world_item_disappearance_ttl_seconds", 5.0)
    _set_if_missing(sender, "require_world_item_confirmation", True)
    _set_if_missing(sender, "last_world_item_scan_count", 0)
    _set_if_missing(sender, "enable_delivery_ack", True)
    _set_if_missing(sender, "retry_interval_seconds", 1.0)
    _set_if_missing(sender, "max_retry_attempts", 12)
    _set_if_missing(sender, "enable_perf_logs", False)
    _set_if_missing(sender, "event_sequence", 0)
    _set_if_missing(sender, "last_seen_map_id", 0)
    _set_if_missing(sender, "last_seen_instance_uptime_ms", 0)
    _set_if_missing(sender, "sender_session_id", 1)
    _set_if_missing(sender, "sender_runtime_generation", int(sender_runtime_generation))
    _set_if_missing(
        sender,
        "sender_runtime_id",
        f"sender-g{int(getattr(sender, 'sender_runtime_generation', sender_runtime_generation) or sender_runtime_generation)}-p{int(os.getpid())}",
    )
    _set_if_missing(sender, "sender_tick_sequence", 0)
    _set_if_missing(sender, "pending_reset_origin", "")
    _set_if_missing(sender, "pending_reset_source_runtime_id", "")
    _set_if_missing(sender, "pending_reset_source_caller", "")
    _set_if_missing(sender, "pending_reset_source_sequence", 0)
    _set_if_missing(sender, "last_session_transition_reason", "")
    _set_if_missing(
        sender,
        "runtime_config_path",
        os.path.join(os.path.dirname(constants.DROP_LOG_PATH), "drop_tracker_runtime_config.json"),
    )
    _set_if_missing(sender, "last_inventory_activity_ts", 0.0)
    _set_if_missing(sender, "last_heartbeat_log_at", 0.0)
    _set_if_missing(sender, "heartbeat_interval_s", 2.5)
    _set_if_missing(sender, "startup_stable_snapshot_credit", 0)
    _set_if_missing(sender, "sent_event_stats_cache", {})
    _set_if_missing(sender, "sent_event_stats_ttl_seconds", 600.0)
    _set_if_missing(sender, "max_stats_builds_per_tick", 2)
    _set_if_missing(sender, "name_refresh_ttl_seconds", 4.0)
    _set_if_missing(sender, "name_refresh_poll_interval_seconds", 0.25)
    _set_if_missing(sender, "max_name_refresh_per_tick", 4)
    _set_if_missing(sender, "refresh_stats_after_name_refresh", True)
    _set_if_missing(sender, "world_item_poll_timer", ThrottledTimer(150))
    _set_if_missing(sender, "debug_enabled", False)
    _set_if_missing(sender, "inventory_poll_timer", ThrottledTimer(250))
    sender.state_version = int(state_version)


def initialize_sender_runtime_state(sender, state_version: int, sender_runtime_generation: int) -> None:
    sender._initialized = True
    sender.state_version = int(state_version)
    sender.inventory_poll_timer = ThrottledTimer(250)
    sender.world_item_poll_timer = ThrottledTimer(150)
    sender.last_inventory_snapshot = {}
    sender.enabled = True
    sender.last_snapshot_total = 0
    sender.last_snapshot_ready = 0
    sender.last_snapshot_not_ready = 0
    sender.last_sent_count = 0
    sender.last_candidate_count = 0
    sender.last_enqueued_count = 0
    sender.is_warmed_up = False
    sender.stable_snapshot_count = 0
    sender.pending_slot_deltas = {}
    sender.carryover_inventory_snapshot = {}
    sender.carryover_suppression_until = 0.0
    sender.current_world_item_agents = {}
    sender.recent_world_item_disappearances = []
    sender.world_item_seen_since_reset = False
    sender.world_item_disappearance_ttl_seconds = 5.0
    sender.require_world_item_confirmation = True
    sender.last_world_item_scan_count = 0
    sender.outbox_queue = []
    sender.pending_name_refresh_by_event = {}
    sender.max_send_per_tick = 12
    sender.max_outbox_size = 2000
    sender.max_snapshot_size_jump = 40
    sender.warmup_grace_seconds = 3.0
    sender.warmup_grace_until = 0.0
    sender.session_startup_pending = False
    sender.pending_ttl_seconds = 6.0
    sender.debug_pipeline_logs = False
    sender.live_debug_detailed = True
    sender.last_known_is_leader = False
    sender.current_receiver_email = ""
    sender.last_reset_reason = ""
    sender.last_reset_map_id = 0
    sender.last_reset_instance_uptime_ms = 0
    sender.last_reset_started_at = 0.0
    sender.enable_delivery_ack = True
    sender.retry_interval_seconds = 1.0
    sender.max_retry_attempts = 12
    sender.enable_perf_logs = False
    sender.event_sequence = 0
    sender.last_seen_map_id = 0
    sender.last_seen_instance_uptime_ms = 0
    sender.sender_session_id = 1
    sender.sender_runtime_generation = int(sender_runtime_generation)
    sender.sender_runtime_id = f"sender-g{int(sender.sender_runtime_generation)}-p{int(os.getpid())}"
    sender.sender_tick_sequence = 0
    sender.pending_reset_origin = ""
    sender.pending_reset_source_runtime_id = ""
    sender.pending_reset_source_caller = ""
    sender.pending_reset_source_sequence = 0
    sender.last_session_transition_reason = ""
    sender.last_process_duration_ms = 0.0
    sender.last_ack_count = 0
    sender.last_inventory_activity_ts = 0.0
    sender.last_heartbeat_log_at = 0.0
    sender.heartbeat_interval_s = 2.5
    sender.startup_stable_snapshot_credit = 0
    sender.sent_event_stats_cache = {}
    sender.sent_event_stats_ttl_seconds = 600.0
    sender.max_stats_builds_per_tick = 2
    sender.name_refresh_ttl_seconds = 4.0
    sender.name_refresh_poll_interval_seconds = 0.25
    sender.max_name_refresh_per_tick = 4
    sender.refresh_stats_after_name_refresh = True
    sender.debug_reset_trace_until = 0.0
    sender.debug_reset_trace_snapshot_logs_remaining = 0
    sender.debug_reset_trace_event_logs_remaining = 0
    sender.debug_reset_trace_lines = []
    sender.runtime_config_path = os.path.join(
        os.path.dirname(constants.DROP_LOG_PATH),
        "drop_tracker_runtime_config.json",
    )
    sender.live_debug_log_path = get_live_debug_log_path(constants.DROP_LOG_PATH)
    sender.ack_poll_timer = ThrottledTimer(250)
    sender.config_poll_timer = ThrottledTimer(2000)
    sender.mod_db = None
