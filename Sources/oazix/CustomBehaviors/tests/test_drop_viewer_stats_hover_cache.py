from __future__ import annotations

from dataclasses import dataclass

from Sources.oazix.CustomBehaviors.skills.monitoring import drop_viewer_draw_panels
from Sources.oazix.CustomBehaviors.skills.monitoring import drop_viewer_draw_tables


@dataclass
class _ParsedRow:
    item_name: str
    player_name: str = "Player"
    quantity: int = 1
    rarity: str = "Unknown"


class _FakeViewer:
    def __init__(self) -> None:
        self.stats_calls = 0
        self._ui_row_stats_cache = {}
        self.stats_by_event = {}
        self.stats_payload_by_event = {}
        self._event_state_stats = {}
        self._event_state_payload = {}

    def _ensure_text(self, value) -> str:
        return str(value or "")

    def _extract_row_event_id(self, row) -> str:
        return str(row.get("event_id", "") or "")

    def _extract_row_sender_email(self, row) -> str:
        return str(row.get("sender_email", "") or "")

    def _extract_row_item_id(self, row) -> int:
        return int(row.get("item_id", 0) or 0)

    def _extract_row_item_stats(self, row) -> str:
        return str(row.get("item_stats", "") or "")

    def _parse_drop_row(self, row):
        return _ParsedRow(
            item_name=str(row.get("item_name", "") or ""),
            player_name=str(row.get("player_name", "") or "Player"),
            quantity=int(row.get("quantity", 1) or 1),
            rarity=str(row.get("rarity", "") or "Unknown"),
        )

    def _clean_item_name(self, value) -> str:
        return str(value or "").strip()

    def _display_player_name(self, player_name: str, sender_email: str = "") -> str:
        return str(player_name or sender_email or "Unknown")

    def _get_row_stats_text(self, row) -> str:
        self.stats_calls += 1
        return f"stats-call-{self.stats_calls}"

    def _resolve_stats_cache_key_for_row(self, row) -> str:
        return str(row.get("event_id", "") or "")

    def _get_event_state_stats_text(self, cache_key: str) -> str:
        return str(self._event_state_stats.get(cache_key, "") or "")

    def _get_event_state_payload_text(self, cache_key: str) -> str:
        return str(self._event_state_payload.get(cache_key, "") or "")

    def _get_cached_stats_text(self, cache: dict, cache_key: str) -> str:
        return str(cache.get(cache_key, "") or "")

    def _render_payload_stats_cached(
        self,
        cache_key: str,
        payload_text: str,
        fallback_item_name: str = "",
        owner_name: str = "",
    ) -> str:
        _ = (cache_key, fallback_item_name, owner_name)
        return str(payload_text or "")


def _make_row() -> dict:
    return {
        "event_id": "ev-123",
        "sender_email": "sender@example.com",
        "item_id": 101,
        "item_stats": "",
        "item_name": "Icy Lodestone",
        "quantity": 2,
        "rarity": "White",
        "player_name": "Mesmer Sedam",
    }


def test_draw_tables_hover_stats_cache_reuses_within_ttl(monkeypatch):
    viewer = _FakeViewer()
    row = _make_row()
    now = [100.0]
    monkeypatch.setattr(drop_viewer_draw_tables.time, "time", lambda: now[0])

    first = drop_viewer_draw_tables._get_row_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.35,
    )
    second = drop_viewer_draw_tables._get_row_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.35,
    )

    assert first == "stats-call-1"
    assert second == "stats-call-1"
    assert viewer.stats_calls == 1


def test_draw_tables_hover_stats_cache_refreshes_after_ttl(monkeypatch):
    viewer = _FakeViewer()
    row = _make_row()
    now = [200.0]
    monkeypatch.setattr(drop_viewer_draw_tables.time, "time", lambda: now[0])

    drop_viewer_draw_tables._get_row_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.20,
    )
    now[0] = 200.25
    refreshed = drop_viewer_draw_tables._get_row_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.20,
    )

    assert refreshed == "stats-call-2"
    assert viewer.stats_calls == 2


def test_draw_panels_selected_stats_cache_reuses_same_row(monkeypatch):
    viewer = _FakeViewer()
    row = _make_row()
    now = [300.0]
    monkeypatch.setattr(drop_viewer_draw_panels.time, "time", lambda: now[0])

    first = drop_viewer_draw_panels._get_row_stats_text_cached(
        viewer,
        row,
        scope="selected_panel",
        refresh_interval_s=0.25,
    )
    row["item_stats"] = "already-rendered"
    second = drop_viewer_draw_panels._get_row_stats_text_cached(
        viewer,
        row,
        scope="selected_panel",
        refresh_interval_s=0.25,
    )

    assert first == "stats-call-1"
    assert second == "stats-call-1"
    assert viewer.stats_calls == 1


def test_draw_tables_preview_cache_uses_existing_row_stats_without_heavy_call(monkeypatch):
    viewer = _FakeViewer()
    row = _make_row()
    row["item_stats"] = "cached-row-stats"
    now = [400.0]
    monkeypatch.setattr(drop_viewer_draw_tables.time, "time", lambda: now[0])

    first = drop_viewer_draw_tables._get_row_preview_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.35,
    )
    second = drop_viewer_draw_tables._get_row_preview_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.35,
    )

    assert first == "cached-row-stats"
    assert second == "cached-row-stats"
    assert viewer.stats_calls == 0


def test_draw_panels_preview_cache_uses_event_state_stats_without_heavy_call(monkeypatch):
    viewer = _FakeViewer()
    row = _make_row()
    viewer._event_state_stats["ev-123"] = "event-state-stats"
    now = [500.0]
    monkeypatch.setattr(drop_viewer_draw_panels.time, "time", lambda: now[0])

    first = drop_viewer_draw_panels._get_row_preview_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.35,
    )
    second = drop_viewer_draw_panels._get_row_preview_stats_text_cached(
        viewer,
        row,
        scope="hover_preview",
        refresh_interval_s=0.35,
    )

    assert first == "event-state-stats"
    assert second == "event-state-stats"
    assert viewer.stats_calls == 0


def test_draw_panels_preview_item_summary_uses_hover_row_only():
    viewer = _FakeViewer()
    row = _make_row()
    summary = drop_viewer_draw_panels._build_preview_item_stats_summary(
        viewer,
        row,
        ("Icy Lodestone", "White"),
    )
    assert summary is not None
    assert summary["name"] == "Icy Lodestone"
    assert summary["rarity"] == "White"
    assert int(summary["quantity"]) == 2
    assert int(summary["count"]) == 1
    assert summary["characters"][0][0] == "Mesmer Sedam"
