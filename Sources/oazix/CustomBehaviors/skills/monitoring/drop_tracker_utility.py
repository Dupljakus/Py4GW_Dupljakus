import os
import re
from typing import Any

from Py4GWCoreLib import GLOBAL_CACHE, Item, Map, Party, Player
from Py4GWCoreLib.Py4GWcorelib import ThrottledTimer
from Py4GWCoreLib.enums import SharedCommandType
from Sources.oazix.CustomBehaviors.primitives import constants
from Sources.oazix.CustomBehaviors.primitives.helpers.custom_behavior_helpers_party import CustomBehaviorHelperParty
from Sources.oazix.CustomBehaviors.primitives.helpers.map_instance_helper import (
    classify_map_instance_transition,
    read_current_map_instance,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_protocol import (
    build_drop_meta,
    build_name_chunks,
    encode_name_chunk_meta,
    make_event_id,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_inventory_runtime import (
    buffer_pending_slot_delta,
    make_orphan_pending_slot_key,
    process_inventory_deltas,
    take_inventory_snapshot,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_world_items import (
    build_world_item_state,
    consume_recent_world_item_confirmation,
    poll_world_item_disappearances,
    prune_recent_world_item_disappearances,
    world_item_names_compatible,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.item_mod_render_utils import (
    build_known_spellcasting_mod_lines,
    prune_generic_attribute_bonus_lines,
    render_mod_description_template,
    sort_stats_lines_like_ingame,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_item_stats_runtime import (
    build_identified_name_from_modifiers,
    build_item_stats_text,
    build_known_spellcast_mod_lines as build_known_spellcast_mod_lines_runtime,
    collect_fallback_mod_lines,
    collect_fallback_rune_lines,
    entry_item_identity_matches,
    extract_parsed_mod_name_parts,
    format_attribute_name,
    load_mod_database,
    match_mod_definition_against_raw,
    normalize_stats_lines,
    prune_generic_attribute_bonus_lines_local,
    render_mod_description_template_local,
    resolve_best_live_item_name,
    resolve_event_item_id_for_stats,
    weapon_mod_type_matches,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_tick_runtime import run_sender_tick
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_sender_state import (
    advance_sender_session_id,
    arm_reset_trace,
    clear_cached_event_stats,
    clear_cached_event_stats_for_item,
    get_cached_event_identity,
    get_cached_event_stats_text,
    log_reset_trace,
    prune_sent_event_stats_cache,
    remember_event_identity,
    remember_event_stats_snapshot,
    reset_trace_active,
    reset_trace_actor_label,
    reset_tracking_state,
    resolve_live_item_id_for_event,
    should_track_name_refresh,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_session_controller import (
    begin_sender_tracking_session,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_session_state import (
    initialize_sender_runtime_state,
    sync_existing_sender_runtime_state,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_sender_transport import (
    build_stats_fallback_text_for_entry,
    flush_outbox,
    is_party_leader_client,
    load_runtime_config,
    log_name_trace,
    next_event_id,
    poll_ack_messages,
    process_pending_name_refreshes,
    queue_drop,
    resolve_current_party_member_emails,
    resolve_party_leader_email,
    schedule_name_refresh_for_entry,
    schedule_name_refresh_for_item,
    send_drop,
    send_name_chunks,
    send_stats_chunks,
)
from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_live_debug import (
    append_live_debug_log,
    clear_live_debug_log,
    get_live_debug_log_path,
)

EXPECTED_RUNTIME_ERRORS = (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError, OSError)
_SENDER_RUNTIME_GENERATION = int(globals().get("_SENDER_RUNTIME_GENERATION", 0) or 0) + 1


class DropTrackerSender:
    """
    Non-blocking shared-memory drop sender.
    Runs from daemon() and never participates in utility score arbitration.
    """

    _instance = None
    _STATE_VERSION = 18

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DropTrackerSender, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        expected_runtime_config_path = os.path.join(
            os.path.dirname(constants.DROP_LOG_PATH),
            "drop_tracker_runtime_config.json",
        )
        expected_live_debug_log_path = get_live_debug_log_path(constants.DROP_LOG_PATH)
        if self._initialized:
            # Hot-reload/session safety: if schema/version changed, force a clean baseline.
            if (
                getattr(self, "state_version", 0) != self._STATE_VERSION
                or str(getattr(self, "runtime_config_path", "") or "").strip() != expected_runtime_config_path
                or str(getattr(self, "live_debug_log_path", "") or "").strip() != expected_live_debug_log_path
            ):
                initialize_sender_runtime_state(
                    self,
                    state_version=self._STATE_VERSION,
                    sender_runtime_generation=_SENDER_RUNTIME_GENERATION,
                )
                self.gold_regex = re.compile(r"^(?:\[([\d: ]+[ap]m)\] )?Your party shares ([\d,]+) gold\.$")
                self.warn_timer = ThrottledTimer(3000)
                self.debug_timer = ThrottledTimer(5000)
                self.snapshot_error_timer = ThrottledTimer(5000)
                self._load_mod_database()
            else:
                sync_existing_sender_runtime_state(
                    self,
                    state_version=self._STATE_VERSION,
                    sender_runtime_generation=_SENDER_RUNTIME_GENERATION,
                )
            return
        initialize_sender_runtime_state(
            self,
            state_version=self._STATE_VERSION,
            sender_runtime_generation=_SENDER_RUNTIME_GENERATION,
        )
        self.gold_regex = re.compile(r"^(?:\[([\d: ]+[ap]m)\] )?Your party shares ([\d,]+) gold\.$")
        self.warn_timer = ThrottledTimer(3000)
        self.debug_timer = ThrottledTimer(5000)
        self.snapshot_error_timer = ThrottledTimer(5000)
        self._load_mod_database()

    def _load_mod_database(self):
        return load_mod_database(self)

    def _format_attribute_name(self, attr_name: str) -> str:
        return format_attribute_name(self, attr_name)

    def _render_mod_description_template(
        self,
        description: str,
        matched_modifiers: list[tuple[int, int, int]],
        default_value: int = 0,
        attribute_name: str = "",
    ) -> list[str]:
        return render_mod_description_template_local(
            self,
            description,
            matched_modifiers,
            default_value=default_value,
            attribute_name=attribute_name,
        )

    def _match_mod_definition_against_raw(self, definition_modifiers, raw_mods) -> list[tuple[int, int, int]]:
        return match_mod_definition_against_raw(self, definition_modifiers, raw_mods)

    def _weapon_mod_type_matches(self, weapon_mod, item_type) -> bool:
        return weapon_mod_type_matches(self, weapon_mod, item_type)

    def _collect_fallback_mod_lines(self, raw_mods, item_attr_txt: str, item_type=None) -> list[str]:
        return collect_fallback_mod_lines(self, raw_mods, item_attr_txt, item_type)

    def _collect_fallback_rune_lines(self, raw_mods, item_attr_txt: str) -> list[str]:
        return collect_fallback_rune_lines(self, raw_mods, item_attr_txt)

    def _build_known_spellcast_mod_lines(self, raw_mods, item_attr_txt: str, item_type=None) -> list[str]:
        return build_known_spellcast_mod_lines_runtime(self, raw_mods, item_attr_txt, item_type)

    def _prune_generic_attribute_bonus_lines(self, lines: list[str]) -> list[str]:
        return prune_generic_attribute_bonus_lines_local(self, lines)

    def _normalize_stats_lines(self, lines: list[str]) -> list[str]:
        return normalize_stats_lines(self, lines)

    def _extract_parsed_mod_name_parts(self, parsed_result) -> tuple[str, str, str]:
        prefix = ""
        suffix = ""
        inherent = ""
        try:
            parsed_prefix = getattr(parsed_result, "prefix", None)
            if parsed_prefix is not None:
                prefix = str(
                    getattr(getattr(parsed_prefix, "weapon_mod", None), "name", "")
                    or getattr(getattr(parsed_prefix, "rune", None), "name", "")
                    or ""
                ).strip()
            parsed_suffix = getattr(parsed_result, "suffix", None)
            if parsed_suffix is not None:
                suffix = str(
                    getattr(getattr(parsed_suffix, "weapon_mod", None), "name", "")
                    or getattr(getattr(parsed_suffix, "rune", None), "name", "")
                    or ""
                ).strip()
            parsed_inherent = getattr(parsed_result, "inherent", None)
            if parsed_inherent is not None:
                inherent = str(
                    getattr(getattr(parsed_inherent, "weapon_mod", None), "name", "")
                    or getattr(getattr(parsed_inherent, "rune", None), "name", "")
                    or ""
                ).strip()
        except EXPECTED_RUNTIME_ERRORS:
            return "", "", ""
        return prefix, suffix, inherent

    def _build_identified_name_from_modifiers(self, *args, **kwargs) -> str:
        return build_identified_name_from_modifiers(self, *args, **kwargs)

    def _resolve_best_live_item_name(self, item_id: int, fallback_name: str = "") -> str:
        return resolve_best_live_item_name(self, item_id, fallback_name)

    def _build_item_stats_text(self, item_id: int, item_name: str = "") -> str:
        return build_item_stats_text(self, item_id, item_name)

    def _entry_item_identity_matches(
        self,
        item_id: int,
        expected_model_id: int,
        expected_name_signature: str,
        expected_item_name: str = "",
        allow_unready_name: bool = False,
    ) -> bool:
        return entry_item_identity_matches(
            self,
            item_id,
            expected_model_id,
            expected_name_signature,
            expected_item_name=expected_item_name,
            allow_unready_name=allow_unready_name,
        )

    def _resolve_event_item_id_for_stats(self, entry: dict) -> int:
        return resolve_event_item_id_for_stats(self, entry)

    def _reset_tracking_state(self, clear_outbox: bool = True):
        return reset_tracking_state(self, clear_outbox=clear_outbox)

    def _arm_reset_trace(self, reason: str, current_map_id: int = 0, current_instance_uptime_ms: int = 0):
        return arm_reset_trace(self, reason, current_map_id=current_map_id, current_instance_uptime_ms=current_instance_uptime_ms)

    def _reset_trace_active(self) -> bool:
        return reset_trace_active(self)

    def _reset_trace_actor_label(self) -> str:
        return reset_trace_actor_label(self)

    def _advance_sender_session_id(self) -> int:
        return advance_sender_session_id(self)

    def _begin_new_session(self, reason: str, current_map_id: int = 0, current_instance_uptime_ms: int = 0):
        return begin_sender_tracking_session(
            self,
            reason,
            current_map_id=current_map_id,
            current_instance_uptime_ms=current_instance_uptime_ms,
        )

    def _append_live_debug_log(self, event: str, message: str, **fields: Any):
        if "sender_runtime_id" not in fields:
            fields["sender_runtime_id"] = str(getattr(self, "sender_runtime_id", "") or "")
        if "sender_runtime_generation" not in fields:
            fields["sender_runtime_generation"] = int(getattr(self, "sender_runtime_generation", 0) or 0)
        return append_live_debug_log(
            actor="sender",
            event=event,
            message=message,
            drop_log_path=constants.DROP_LOG_PATH,
            **fields,
        )

    def _clear_live_debug_log(self):
        return clear_live_debug_log(constants.DROP_LOG_PATH)

    def _log_reset_trace(
        self,
        message: str,
        consume_snapshot: bool = False,
        consume_event: bool = False,
        level=None,
    ):
        if consume_event:
            remaining = int(getattr(self, "debug_reset_trace_event_logs_remaining", 0) or 0)
            if remaining > 0:
                self.debug_reset_trace_event_logs_remaining = remaining - 1
        trace_lines = getattr(self, "debug_reset_trace_lines", None)
        if not isinstance(trace_lines, list):
            self.debug_reset_trace_lines = []
            trace_lines = self.debug_reset_trace_lines
        trace_lines.append(str(message or ""))
        if len(trace_lines) > 120:
            del trace_lines[:-120]
        return log_reset_trace(self, message, consume_snapshot=consume_snapshot)

    def _strip_tags(self, text: str) -> str:
        return re.sub(r"<[^>]+>", "", text or "")

    def _prune_recent_world_item_disappearances(self, now_ts: float | None = None):
        prune_recent_world_item_disappearances(self, now_ts)

    def _build_world_item_state(self, agent_id: int) -> dict[str, Any] | None:
        return build_world_item_state(self, agent_id)

    def _poll_world_item_disappearances(self):
        poll_world_item_disappearances(self)

    def _world_item_names_compatible(self, world_name: str, event_name: str) -> bool:
        return world_item_names_compatible(world_name, event_name)

    def _consume_recent_world_item_confirmation(self, event: dict[str, Any]) -> bool:
        return consume_recent_world_item_confirmation(self, event)

    def _prune_sent_event_stats_cache(self, now_ts: float | None = None):
        return prune_sent_event_stats_cache(self, now_ts)

    def _remember_event_identity(
        self,
        event_id: str,
        item_id: int,
        model_id: int,
        item_name: str,
        name_signature: str = "",
        rarity: str = "",
        last_receiver_email: str = "",
    ):
        return remember_event_identity(
            self,
            event_id,
            item_id,
            model_id,
            item_name,
            name_signature=name_signature,
            rarity=rarity,
            last_receiver_email=last_receiver_email,
        )

    def get_cached_event_identity(self, event_id: str) -> dict:
        return get_cached_event_identity(self, event_id)

    def resolve_live_item_id_for_event(self, event_id: str, preferred_item_id: int = 0) -> int:
        return resolve_live_item_id_for_event(self, event_id, preferred_item_id=preferred_item_id)

    def clear_cached_event_stats(self, event_id: str, item_id: int = 0):
        return clear_cached_event_stats(self, event_id, item_id=item_id)

    def clear_cached_event_stats_for_item(self, item_id: int = 0, model_id: int = 0):
        return clear_cached_event_stats_for_item(self, item_id=item_id, model_id=model_id)

    def _remember_event_stats_snapshot(
        self,
        event_id: str,
        item_id: int,
        model_id: int,
        item_name: str,
        stats_text: str,
        name_signature: str = "",
        rarity: str = "",
        last_receiver_email: str = "",
    ):
        return remember_event_stats_snapshot(
            self,
            event_id,
            item_id,
            model_id,
            item_name,
            stats_text,
            name_signature=name_signature,
            rarity=rarity,
            last_receiver_email=last_receiver_email,
        )

    def _should_track_name_refresh(self, item_name: str = "", rarity: str = "") -> bool:
        return should_track_name_refresh(self, item_name=item_name, rarity=rarity)

    def get_cached_event_stats_text(self, event_id: str, item_id: int = 0, model_id: int = 0) -> str:
        return get_cached_event_stats_text(self, event_id, item_id=item_id, model_id=model_id)

    def _make_orphan_pending_slot_key(self, item_id: int, now_ts: float) -> tuple[int, int]:
        return make_orphan_pending_slot_key(self, item_id, now_ts)

    def _buffer_pending_slot_delta(
        self,
        slot_key: tuple[int, int],
        delta_qty: int,
        model_id: int,
        item_id: int,
        rarity: str,
        now_ts: float,
    ):
        return buffer_pending_slot_delta(
            self,
            slot_key,
            delta_qty,
            model_id,
            item_id,
            rarity,
            now_ts,
        )

    def _resolve_party_leader_email(self) -> str | None:
        return resolve_party_leader_email(self)

    def _resolve_current_party_member_emails(self) -> list[str]:
        return resolve_current_party_member_emails(self)

    def _is_party_leader_client(self) -> bool:
        return is_party_leader_client(self)

    def _next_event_id(self) -> str:
        return next_event_id(self)

    def _load_runtime_config(self):
        return load_runtime_config(self)

    def _send_name_chunks(
        self,
        receiver_email: str,
        my_email: str,
        name_signature: str,
        full_name: str,
        event_id: str = "",
    ) -> bool:
        return send_name_chunks(self, receiver_email, my_email, name_signature, full_name, event_id=event_id)

    def _log_name_trace(self, message: str) -> None:
        return log_name_trace(self, message)

    def _schedule_name_refresh_for_entry(self, entry: dict, receiver_email: str = "") -> None:
        return schedule_name_refresh_for_entry(self, entry, receiver_email=receiver_email)

    def _process_pending_name_refreshes(self) -> int:
        return process_pending_name_refreshes(self)

    def schedule_name_refresh_for_item(self, item_id: int = 0, model_id: int = 0) -> int:
        return schedule_name_refresh_for_item(self, item_id=item_id, model_id=model_id)

    def _send_stats_chunks(self, receiver_email: str, my_email: str, event_id: str, stats_text: str) -> bool:
        return send_stats_chunks(self, receiver_email, my_email, event_id, stats_text)

    def _build_stats_fallback_text_for_entry(self, entry: dict[str, Any]) -> str:
        return build_stats_fallback_text_for_entry(self, entry)

    def _send_drop(
        self,
        item_name: str,
        quantity: int,
        rarity: str,
        display_time: str = "",
        item_id: int = 0,
        model_id: int = 0,
        slot_bag: int = 0,
        slot_index: int = 0,
        is_leader_sender: bool = False,
        event_id: str = "",
        name_signature: str = "",
        sender_session_id: int = 0,
    ) -> bool:
        return send_drop(
            self,
            item_name,
            quantity,
            rarity,
            display_time=display_time,
            item_id=item_id,
            model_id=model_id,
            slot_bag=slot_bag,
            slot_index=slot_index,
            is_leader_sender=is_leader_sender,
            event_id=event_id,
            name_signature=name_signature,
            sender_session_id=sender_session_id,
        )

    def _queue_drop(
        self,
        item_name: str,
        quantity: int,
        rarity: str,
        display_time: str,
        item_id: int = 0,
        model_id: int = 0,
        slot_key: tuple[int, int] | None = None,
        reason: str = "delta",
        is_leader_sender: bool = False,
    ):
        return queue_drop(
            self,
            item_name,
            quantity,
            rarity,
            display_time,
            item_id=item_id,
            model_id=model_id,
            slot_key=slot_key,
            reason=reason,
            is_leader_sender=is_leader_sender,
        )

    def _poll_ack_messages(self) -> int:
        return poll_ack_messages(self)

    def _flush_outbox(self) -> int:
        return flush_outbox(self)

    def _take_inventory_snapshot(self) -> dict[tuple[int, int], tuple[str, str, int, int, int]]:
        return take_inventory_snapshot(self)

    def _process_inventory_deltas(self):
        return process_inventory_deltas(self)

    def act(self):
        return run_sender_tick(self)


