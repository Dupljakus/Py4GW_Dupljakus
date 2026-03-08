import importlib
import sys
import types


class _FakeSender:
    def __init__(self) -> None:
        self.live_debug_detailed = True
        self.debug_pipeline_logs = False
        self.calls: list[tuple[str, str, dict]] = []

    def _append_live_debug_log(self, event: str, message: str, **fields):
        self.calls.append((str(event), str(message), dict(fields)))


class _FakeWorldConfirmSender(_FakeSender):
    def __init__(self, *, world_seen: bool, startup_pending: bool, carryover_snapshot: dict):
        super().__init__()
        self.world_item_seen_since_reset = bool(world_seen)
        self.session_startup_pending = bool(startup_pending)
        self.carryover_inventory_snapshot = dict(carryover_snapshot or {})
        self.recent_world_item_disappearances = []
        self.current_world_item_agents = {}

    def _consume_recent_world_item_confirmation(self, _event):
        return False


def _import_pipeline_module(monkeypatch):
    py4gw_core = types.ModuleType("Py4GWCoreLib")
    py4gw_core.Py4GW = types.SimpleNamespace(
        Console=types.SimpleNamespace(
            Log=lambda *_args, **_kwargs: None,
            MessageType=types.SimpleNamespace(Info=0),
        )
    )
    monkeypatch.setitem(sys.modules, "Py4GWCoreLib", py4gw_core)
    module_name = "Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_candidate_pipeline"
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_candidate_pipeline_summary_uses_global_throttle_key(monkeypatch):
    pipeline = _import_pipeline_module(monkeypatch)

    sender = _FakeSender()
    candidate_events = [{"reason": "new_slot", "name": "Holy Staff", "qty": 1, "item_id": 42, "model_id": 500}]

    pipeline.log_candidate_pipeline(
        sender,
        candidate_events=candidate_events,
        suppressed_by_model_delta=2,
        suppressed_world_events=[{"reason": "stack_increase"}],
    )

    summary_calls = [call for call in sender.calls if call[0] == "candidate_pipeline_summary"]
    assert len(summary_calls) == 1
    _event, _message, fields = summary_calls[0]
    assert fields.get("dedupe_key") == "candidate_pipeline_summary"
    assert float(fields.get("dedupe_interval_s", 0.0)) == 2.0


def test_candidate_pipeline_summary_emits_only_on_state_change(monkeypatch):
    pipeline = _import_pipeline_module(monkeypatch)
    sender = _FakeSender()
    candidate_events = [{"reason": "new_slot", "name": "Holy Staff", "qty": 1, "item_id": 42, "model_id": 500}]

    pipeline.log_candidate_pipeline(
        sender,
        candidate_events=candidate_events,
        suppressed_by_model_delta=0,
        suppressed_world_events=[],
    )
    pipeline.log_candidate_pipeline(
        sender,
        candidate_events=candidate_events,
        suppressed_by_model_delta=0,
        suppressed_world_events=[],
    )
    pipeline.log_candidate_pipeline(
        sender,
        candidate_events=[],
        suppressed_by_model_delta=0,
        suppressed_world_events=[],
    )

    summary_calls = [call for call in sender.calls if call[0] == "candidate_pipeline_summary"]
    assert len(summary_calls) == 2


def test_confirm_candidate_events_allows_slot_replaced_fallback_when_world_signal_present(monkeypatch):
    pipeline = _import_pipeline_module(monkeypatch)
    sender = _FakeWorldConfirmSender(
        world_seen=True,
        startup_pending=False,
        carryover_snapshot={},
    )
    sender.recent_world_item_disappearances = [1234]
    candidate_events = [
        {
            "reason": "slot_replaced",
            "name": "Stone Summit Badge",
            "qty": 1,
            "item_id": 59,
            "model_id": 502,
            "rarity": "White",
            "slot_key": (2, 3),
        }
    ]

    confirmed, suppressed_model_delta, suppressed_world = pipeline.confirm_candidate_events(
        sender=sender,
        candidate_events=candidate_events,
        prev_model_qty={502: 1},
        current_model_qty={502: 1},
        prev_item_ids={11, 12, 13},
        require_world_confirmation=True,
    )

    assert suppressed_model_delta == 0
    assert suppressed_world == []
    assert len(confirmed) == 1
    assert str(confirmed[0].get("reason", "")) == "slot_replaced"


def test_confirm_candidate_events_allows_slot_replaced_without_world_signal(monkeypatch):
    pipeline = _import_pipeline_module(monkeypatch)
    sender = _FakeWorldConfirmSender(
        world_seen=True,
        startup_pending=False,
        carryover_snapshot={},
    )
    sender.recent_world_item_disappearances = []
    sender.current_world_item_agents = {}
    candidate_events = [
        {
            "reason": "slot_replaced",
            "name": "Stone Summit Badge",
            "qty": 1,
            "item_id": 59,
            "model_id": 502,
            "rarity": "White",
            "slot_key": (2, 3),
        }
    ]

    confirmed, suppressed_model_delta, suppressed_world = pipeline.confirm_candidate_events(
        sender=sender,
        candidate_events=candidate_events,
        prev_model_qty={502: 1},
        current_model_qty={502: 1},
        prev_item_ids={11, 12, 13},
        require_world_confirmation=True,
    )

    assert suppressed_model_delta == 0
    assert suppressed_world == []
    assert len(confirmed) == 1
    assert str(confirmed[0].get("reason", "")) == "slot_replaced"


def test_confirm_candidate_events_keeps_slot_replaced_suppressed_while_carryover_active(monkeypatch):
    pipeline = _import_pipeline_module(monkeypatch)
    sender = _FakeWorldConfirmSender(
        world_seen=True,
        startup_pending=False,
        carryover_snapshot={(1, 1): ("Old Item", "White", 1, 500, 42)},
    )
    candidate_events = [
        {
            "reason": "slot_replaced",
            "name": "Stone Summit Badge",
            "qty": 1,
            "item_id": 59,
            "model_id": 502,
            "rarity": "White",
            "slot_key": (2, 3),
        }
    ]

    confirmed, suppressed_model_delta, suppressed_world = pipeline.confirm_candidate_events(
        sender=sender,
        candidate_events=candidate_events,
        prev_model_qty={502: 1},
        current_model_qty={502: 1},
        prev_item_ids={11, 12, 13},
        require_world_confirmation=True,
    )

    assert suppressed_model_delta == 0
    assert confirmed == []
    assert len(suppressed_world) == 1
