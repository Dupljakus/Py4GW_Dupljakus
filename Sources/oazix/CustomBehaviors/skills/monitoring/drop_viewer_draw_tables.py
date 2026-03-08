import sys
import time


def _viewer_runtime_module(viewer):
    try:
        return sys.modules.get(viewer.__class__.__module__)
    except (TypeError, ValueError, RuntimeError, AttributeError):
        return None


def _runtime_attr(viewer, name: str, fallback=None):
    module = _viewer_runtime_module(viewer)
    if module is not None and hasattr(module, name):
        return getattr(module, name)
    return fallback


def _table_row_min_height(pyimgui) -> float:
    get_line_height_fn = getattr(pyimgui, "get_text_line_height_with_spacing", None)
    if callable(get_line_height_fn):
        try:
            return max(18.0, float(get_line_height_fn()) + 2.0)
        except (TypeError, ValueError, RuntimeError, AttributeError):
            return 20.0
    return 20.0


def _table_row_is_visible(pyimgui, row_height: float) -> bool:
    is_rect_visible_fn = getattr(pyimgui, "is_rect_visible", None)
    if callable(is_rect_visible_fn):
        try:
            return bool(is_rect_visible_fn(1.0, float(row_height)))
        except (TypeError, ValueError, RuntimeError, AttributeError):
            return True
    return True


def _hover_preview_ready(viewer, hover_key) -> bool:
    if not hover_key:
        return False
    try:
        now_ts = float(time.time())
    except (TypeError, ValueError, RuntimeError, AttributeError):
        now_ts = 0.0
    try:
        debounce_s = max(0.0, min(0.4, float(getattr(viewer, "hover_preview_debounce_s", 0.14) or 0.14)))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        debounce_s = 0.14

    candidate_key = getattr(viewer, "hover_preview_debounce_key", None)
    if candidate_key != hover_key:
        viewer.hover_preview_debounce_key = hover_key
        viewer.hover_preview_debounce_ts = now_ts
        return debounce_s <= 0.0

    try:
        candidate_ts = float(getattr(viewer, "hover_preview_debounce_ts", 0.0) or 0.0)
    except (TypeError, ValueError, RuntimeError, AttributeError):
        candidate_ts = 0.0
    return (now_ts - candidate_ts) >= debounce_s


def _build_item_hover_tooltip_text(viewer, row, fallback_item_name: str = "") -> str:
    fallback_name = viewer._clean_item_name(fallback_item_name)
    if row is not None:
        stats_text = _get_row_preview_stats_text_cached(
            viewer,
            row,
            scope="hover_preview",
            refresh_interval_s=0.35,
        )
        if stats_text:
            return stats_text
        parsed = viewer._parse_drop_row(row)
        if parsed is not None:
            parsed_name = viewer._clean_item_name(getattr(parsed, "item_name", ""))
            if parsed_name:
                fallback_name = parsed_name
    if fallback_name:
        return f"{fallback_name}\nNo stats available yet."
    return "No stats available yet."


def _row_stats_cache_entry_key(viewer, row) -> tuple:
    if row is None:
        return ()
    try:
        event_id = viewer._ensure_text(viewer._extract_row_event_id(row)).strip()
    except (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError):
        event_id = ""
    try:
        sender_email = viewer._ensure_text(viewer._extract_row_sender_email(row)).strip().lower()
    except (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError):
        sender_email = ""
    try:
        item_id = max(0, int(viewer._extract_row_item_id(row)))
    except (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError):
        item_id = 0
    try:
        row_stats = viewer._ensure_text(viewer._extract_row_item_stats(row)).strip()
    except (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError):
        row_stats = ""
    try:
        parsed = viewer._parse_drop_row(row)
    except (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError):
        parsed = None
    row_item_name = ""
    if parsed is not None:
        try:
            row_item_name = viewer._clean_item_name(getattr(parsed, "item_name", ""))
        except (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError):
            row_item_name = ""
    return (
        id(row),
        event_id,
        sender_email,
        item_id,
        row_item_name,
    )


def _get_row_stats_text_cached(viewer, row, scope: str, refresh_interval_s: float = 0.35) -> str:
    if row is None:
        return ""
    try:
        now_ts = float(time.time())
    except (TypeError, ValueError, RuntimeError, AttributeError):
        now_ts = 0.0
    try:
        refresh_interval = max(0.05, float(refresh_interval_s))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        refresh_interval = 0.35

    cache_map = getattr(viewer, "_ui_row_stats_cache", None)
    if not isinstance(cache_map, dict):
        cache_map = {}
        setattr(viewer, "_ui_row_stats_cache", cache_map)

    scope_key = viewer._ensure_text(scope).strip() or "default"
    row_key = _row_stats_cache_entry_key(viewer, row)
    cached_entry = cache_map.get(scope_key)
    if isinstance(cached_entry, dict):
        cached_key = cached_entry.get("key")
        cached_ts = float(cached_entry.get("ts", 0.0) or 0.0)
        if cached_key == row_key and (now_ts - cached_ts) < refresh_interval:
            return viewer._ensure_text(cached_entry.get("text", "")).strip()

    stats_text = viewer._ensure_text(viewer._get_row_stats_text(row)).strip()
    cache_map[scope_key] = {
        "key": row_key,
        "text": stats_text,
        "ts": now_ts,
    }
    return stats_text


def _get_row_preview_stats_text_fast(viewer, row, fallback_item_name: str = "") -> str:
    if row is None:
        return ""
    direct_text = viewer._ensure_text(viewer._extract_row_item_stats(row)).strip()
    if direct_text:
        return direct_text

    event_cache_key = viewer._resolve_stats_cache_key_for_row(row)
    if event_cache_key:
        cached_state = viewer._ensure_text(viewer._get_event_state_stats_text(event_cache_key)).strip()
        if cached_state:
            return cached_state

        payload_text = viewer._ensure_text(viewer._get_event_state_payload_text(event_cache_key)).strip()
        if not payload_text:
            payload_text = viewer._ensure_text(
                viewer._get_cached_stats_text(viewer.stats_payload_by_event, event_cache_key)
            ).strip()
        if payload_text:
            parsed = viewer._parse_drop_row(row)
            owner_name = viewer._ensure_text(getattr(parsed, "player_name", "") if parsed is not None else "").strip()
            fallback_name = viewer._ensure_text(
                getattr(parsed, "item_name", "") if parsed is not None else fallback_item_name
            ).strip()
            rendered = viewer._ensure_text(
                viewer._render_payload_stats_cached(
                    event_cache_key,
                    payload_text,
                    fallback_name,
                    owner_name=owner_name,
                )
            ).strip()
            if rendered:
                return rendered

        cached_text = viewer._ensure_text(
            viewer._get_cached_stats_text(viewer.stats_by_event, event_cache_key)
        ).strip()
        if cached_text:
            return cached_text

    viewer._request_remote_stats_for_row(row, force_refresh=False)
    return ""


def _get_row_preview_stats_text_cached(viewer, row, scope: str, refresh_interval_s: float = 0.35) -> str:
    if row is None:
        return ""
    try:
        now_ts = float(time.time())
    except (TypeError, ValueError, RuntimeError, AttributeError):
        now_ts = 0.0
    try:
        refresh_interval = max(0.05, float(refresh_interval_s))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        refresh_interval = 0.35

    cache_map = getattr(viewer, "_ui_row_stats_cache", None)
    if not isinstance(cache_map, dict):
        cache_map = {}
        setattr(viewer, "_ui_row_stats_cache", cache_map)

    scope_key = f"preview:{viewer._ensure_text(scope).strip() or 'default'}"
    row_key = _row_stats_cache_entry_key(viewer, row)
    cached_entry = cache_map.get(scope_key)
    if isinstance(cached_entry, dict):
        cached_key = cached_entry.get("key")
        cached_ts = float(cached_entry.get("ts", 0.0) or 0.0)
        if cached_key == row_key and (now_ts - cached_ts) < refresh_interval:
            return viewer._ensure_text(cached_entry.get("text", "")).strip()

    parsed = viewer._parse_drop_row(row)
    fallback_name = viewer._ensure_text(getattr(parsed, "item_name", "") if parsed is not None else "").strip()
    stats_text = viewer._ensure_text(_get_row_preview_stats_text_fast(viewer, row, fallback_name)).strip()
    cache_map[scope_key] = {
        "key": row_key,
        "text": stats_text,
        "ts": now_ts,
    }
    return stats_text


def _normalized_item_match_keys(viewer, item_name: str, rarity: str) -> list[tuple[str, str]]:
    rarity_text = viewer._ensure_text(rarity).strip() or "Unknown"
    normalized = viewer._normalize_item_name(item_name)
    if not normalized:
        return []
    keys: list[tuple[str, str]] = [(normalized, rarity_text)]
    if normalized.endswith("s") and len(normalized) > 1:
        singular = normalized[:-1]
        if singular:
            keys.append((singular, rarity_text))
    else:
        keys.append((f"{normalized}s", rarity_text))
    return keys


def _build_best_row_lookup(viewer, rows) -> tuple[dict[tuple[str, str], list], dict[tuple[str, str], list]]:
    best_any_by_key: dict[tuple[str, str], list] = {}
    best_with_item_id_by_key: dict[tuple[str, str], list] = {}
    for row in reversed(list(rows or [])):
        parsed = viewer._parse_drop_row(row)
        if parsed is None:
            continue
        rarity = viewer._ensure_text(parsed.rarity).strip() or "Unknown"
        keys = _normalized_item_match_keys(viewer, viewer._ensure_text(parsed.item_name), rarity)
        if not keys:
            continue
        row_item_id = max(0, int(viewer._extract_row_item_id(row)))
        for key in keys:
            if key not in best_any_by_key:
                best_any_by_key[key] = row
            if row_item_id > 0 and key not in best_with_item_id_by_key:
                best_with_item_id_by_key[key] = row
    return best_any_by_key, best_with_item_id_by_key


def _get_best_row_lookup_cached(viewer, rows) -> tuple[dict[tuple[str, str], list], dict[tuple[str, str], list]]:
    cache_map = getattr(viewer, "_cached_best_row_lookup", None)
    if not isinstance(cache_map, dict):
        cache_map = {}
        setattr(viewer, "_cached_best_row_lookup", cache_map)
    try:
        rows_version = max(0, int(getattr(viewer, "_runtime_rows_version", 0) or 0))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        rows_version = 0
    cache_key = (rows_version, id(rows), len(rows or []))
    cached_key = cache_map.get("key")
    cached_any = cache_map.get("best_any")
    cached_with_item_id = cache_map.get("best_with_item_id")
    if (
        cached_key == cache_key
        and isinstance(cached_any, dict)
        and isinstance(cached_with_item_id, dict)
    ):
        return cached_any, cached_with_item_id

    best_any_by_key, best_with_item_id_by_key = _build_best_row_lookup(viewer, rows)
    cache_map["key"] = cache_key
    cache_map["best_any"] = best_any_by_key
    cache_map["best_with_item_id"] = best_with_item_id_by_key
    return best_any_by_key, best_with_item_id_by_key


def _resolve_best_row_cached(
    viewer,
    *,
    item_name: str,
    rarity: str,
    best_any_by_key: dict[tuple[str, str], list],
    best_with_item_id_by_key: dict[tuple[str, str], list],
):
    keys = _normalized_item_match_keys(viewer, item_name, rarity)
    for key in keys:
        row = best_with_item_id_by_key.get(key)
        if row is not None:
            return row
    for key in keys:
        row = best_any_by_key.get(key)
        if row is not None:
            return row
    return None


def _build_log_row_render_entries(viewer, rows):
    entries = []
    for row_idx, row in enumerate(rows or []):
        parsed = viewer._parse_drop_row(row)
        if parsed is None:
            continue
        rarity = viewer._ensure_text(parsed.rarity).strip() or "Unknown"
        selected_key = (
            viewer._canonical_agg_item_name(parsed.item_name, rarity, viewer.aggregated_drops),
            viewer._ensure_text(rarity).strip() or "Unknown",
        )
        player_label = viewer._display_player_name(
            viewer._ensure_text(parsed.player_name).strip(),
            viewer._extract_row_sender_email(row),
        )
        entries.append(
            (
                row_idx,
                row,
                parsed,
                rarity,
                viewer._get_rarity_color(rarity),
                selected_key,
                player_label,
            )
        )
    return entries


def _get_log_row_render_entries_cached(viewer, rows):
    cache_map = getattr(viewer, "_cached_log_row_entries", None)
    if not isinstance(cache_map, dict):
        cache_map = {}
        setattr(viewer, "_cached_log_row_entries", cache_map)
    try:
        rows_version = max(0, int(getattr(viewer, "_runtime_rows_version", 0) or 0))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        rows_version = 0

    cache_key = (rows_version, id(rows), len(rows or []))
    cached_key = cache_map.get("key")
    cached_entries = cache_map.get("entries")
    if cached_key == cache_key and isinstance(cached_entries, list):
        return cached_entries

    entries = _build_log_row_render_entries(viewer, rows)
    cache_map["key"] = cache_key
    cache_map["entries"] = entries
    return entries


def draw_aggregated(viewer, filtered_rows, materials_only: bool = False) -> None:
    pyimgui = _runtime_attr(viewer, "PyImGui")
    imgui = _runtime_attr(viewer, "ImGui")
    c = viewer._ui_colors()
    filtered_agg, total_filtered_qty = viewer._get_filtered_aggregated(filtered_rows)
    if materials_only:
        filtered_agg = {
            (name, rarity): data
            for (name, rarity), data in filtered_agg.items()
            if viewer._ensure_text(rarity).strip() == "Material"
        }
    else:
        filtered_agg = {
            (name, rarity): data
            for (name, rarity), data in filtered_agg.items()
            if viewer._ensure_text(rarity).strip() != "Material"
        }
    total_filtered_qty = sum(data["Quantity"] for data in filtered_agg.values())
    total_filtered_events = sum(data["Count"] for data in filtered_agg.values())
    total_items_without_gold = total_filtered_qty - sum(
        data["Quantity"] for (name, _), data in filtered_agg.items() if name == "Gold"
    )
    total_events_without_gold = total_filtered_events - sum(
        data["Count"] for (name, _), data in filtered_agg.items() if name == "Gold"
    )

    if materials_only:
        pyimgui.text_colored(
            f"Total Materials (filtered): {max(0, total_items_without_gold)} | Events: {max(0, total_events_without_gold)}",
            c["muted"],
        )
    else:
        pyimgui.text_colored(
            f"Total Items (filtered): {max(0, total_items_without_gold)} | Events: {max(0, total_events_without_gold)}",
            c["muted"],
        )
        pyimgui.same_line(0.0, 12.0)
        viewer._draw_inline_rarity_filter_buttons()
    if not filtered_agg:
        pyimgui.separator()
        if materials_only:
            pyimgui.text_colored("No material drops match your current filters.", c["muted"])
            pyimgui.text("Try clearing filters or switching to Stats/Log tab.")
        else:
            pyimgui.text_colored("No drops match your current filters.", c["muted"])
            pyimgui.text("Try clearing filters or switching to Log tab.")
        return

    pyimgui.push_style_color(pyimgui.ImGuiCol.TableHeaderBg, (0.16, 0.20, 0.27, 0.95))
    if pyimgui.begin_table(
        f"AggTable##{int(viewer._agg_table_reset_nonce)}",
        5,
        pyimgui.TableFlags.Borders | pyimgui.TableFlags.RowBg | pyimgui.TableFlags.Resizable | pyimgui.TableFlags.Sortable | pyimgui.TableFlags.ScrollY,
        0.0,
        360.0,
    ):
        pyimgui.table_setup_column("Item Name")
        pyimgui.table_setup_column("Quantity")
        pyimgui.table_setup_column("%")
        pyimgui.table_setup_column("Rarity")
        pyimgui.table_setup_column("Count")
        pyimgui.table_headers_row()

        display_items = list(filtered_agg.items())
        sorted_items = sorted(display_items, key=lambda x: (x[0][0], x[0][1]))
        best_any_by_key, best_with_item_id_by_key = _get_best_row_lookup_cached(viewer, filtered_rows)
        row_min_height = _table_row_min_height(pyimgui)

        for idx, ((item_name, rarity), data) in enumerate(sorted_items):
            pyimgui.table_next_row(0, row_min_height)
            if not _table_row_is_visible(pyimgui, row_min_height):
                continue
            qty = data["Quantity"]
            if item_name == "Gold":
                pct_str = "---"
            else:
                pct = (qty / total_items_without_gold * 100) if total_items_without_gold > 0 else 0
                pct_str = f"{pct:.2f}%"

            r, g, b, a = viewer._get_rarity_color(rarity)
            row_key = (item_name, rarity)

            pyimgui.table_set_column_index(0)
            pyimgui.push_style_color(pyimgui.ImGuiCol.Text, (r, g, b, a))
            if pyimgui.selectable(
                f"{item_name}##agg_{idx}",
                viewer.selected_item_key == row_key,
                pyimgui.SelectableFlags.NoFlag,
                (0.0, 0.0),
            ):
                viewer.selected_item_key = row_key
                viewer.selected_log_row = _resolve_best_row_cached(
                    viewer,
                    item_name=item_name,
                    rarity=rarity,
                    best_any_by_key=best_any_by_key,
                    best_with_item_id_by_key=best_with_item_id_by_key,
                )
            if pyimgui.is_item_clicked(1):
                pyimgui.open_popup(f"DropAggRowMenu##{idx}")
            if pyimgui.begin_popup(f"DropAggRowMenu##{idx}"):
                target_row = _resolve_best_row_cached(
                    viewer,
                    item_name=item_name,
                    rarity=rarity,
                    best_any_by_key=best_any_by_key,
                    best_with_item_id_by_key=best_with_item_id_by_key,
                )
                if target_row is None:
                    pyimgui.text("No concrete row available")
                else:
                    viewer.selected_item_key = row_key
                    viewer.selected_log_row = target_row
                    if pyimgui.menu_item("Identify item"):
                        viewer._identify_item_for_all_characters(item_name, rarity)
                    if pyimgui.menu_item("Refresh item stats"):
                        viewer._refresh_item_stats_for_all_characters(item_name, rarity)
                pyimgui.end_popup()
            if pyimgui.is_item_hovered():
                hover_row = _resolve_best_row_cached(
                    viewer,
                    item_name=item_name,
                    rarity=rarity,
                    best_any_by_key=best_any_by_key,
                    best_with_item_id_by_key=best_with_item_id_by_key,
                )
                hover_key = (
                    "agg",
                    idx,
                    row_key,
                    viewer._ensure_text(viewer._extract_row_event_id(hover_row) if hover_row is not None else "").strip(),
                    id(hover_row) if hover_row is not None else 0,
                )
                if _hover_preview_ready(viewer, hover_key):
                    viewer._set_hover_item_preview(
                        row_key,
                        hover_row,
                    )
                    imgui.show_tooltip(_build_item_hover_tooltip_text(viewer, hover_row, item_name))
            pyimgui.pop_style_color(1)

            pyimgui.table_set_column_index(1)
            pyimgui.text(str(qty))

            pyimgui.table_set_column_index(2)
            pyimgui.text(pct_str)

            pyimgui.table_set_column_index(3)
            pyimgui.text_colored(rarity, (r, g, b, a))

            pyimgui.table_set_column_index(4)
            pyimgui.text(str(data["Count"]))

        if viewer._request_agg_scroll_bottom:
            pyimgui.set_scroll_here_y(1.0)
            viewer._request_agg_scroll_bottom = False

        pyimgui.end_table()
    pyimgui.pop_style_color(1)
    viewer._draw_selected_item_details()


def draw_log(viewer, filtered_rows) -> None:
    pyimgui = _runtime_attr(viewer, "PyImGui")
    imgui = _runtime_attr(viewer, "ImGui")
    c = viewer._ui_colors()
    viewer._draw_inline_rarity_filter_buttons()
    pyimgui.separator()
    if not filtered_rows:
        pyimgui.text_colored("No log entries to show.", c["muted"])
        pyimgui.text("Drops will appear here as they are tracked.")
        return

    pyimgui.push_style_color(pyimgui.ImGuiCol.TableHeaderBg, (0.16, 0.20, 0.27, 0.95))
    if pyimgui.begin_table(
        f"DropsLogTable##{int(viewer._log_table_reset_nonce)}",
        8,
        pyimgui.TableFlags.Borders | pyimgui.TableFlags.RowBg | pyimgui.TableFlags.Resizable | pyimgui.TableFlags.ScrollY,
        0.0,
        0.0,
    ):
        pyimgui.table_setup_column("Timestamp")
        pyimgui.table_setup_column("Logger")
        pyimgui.table_setup_column("MapID")
        pyimgui.table_setup_column("MapName")
        pyimgui.table_setup_column("Player")
        pyimgui.table_setup_column("Item")
        pyimgui.table_setup_column("Qty")
        pyimgui.table_setup_column("Rarity")
        pyimgui.table_headers_row()
        row_min_height = _table_row_min_height(pyimgui)
        render_entries = _get_log_row_render_entries_cached(viewer, filtered_rows)

        for row_idx, row, parsed, rarity, rarity_color, selected_key, player_label in render_entries:
            pyimgui.table_next_row(0, row_min_height)
            if not _table_row_is_visible(pyimgui, row_min_height):
                continue
            r, g, b, a = rarity_color
            pyimgui.table_set_column_index(0)
            pyimgui.text(viewer._ensure_text(parsed.timestamp).strip())

            pyimgui.table_set_column_index(1)
            pyimgui.text(viewer._ensure_text(parsed.viewer_bot).strip())

            pyimgui.table_set_column_index(2)
            pyimgui.text(str(int(parsed.map_id)))

            pyimgui.table_set_column_index(3)
            pyimgui.text(viewer._ensure_text(parsed.map_name).strip())

            pyimgui.table_set_column_index(4)
            pyimgui.text(player_label)

            pyimgui.table_set_column_index(5)
            pyimgui.push_style_color(pyimgui.ImGuiCol.Text, (r, g, b, a))
            if pyimgui.selectable(
                f"{viewer._ensure_text(parsed.item_name)}##log_item_{row_idx}",
                viewer.selected_item_key == selected_key,
                pyimgui.SelectableFlags.NoFlag,
                (0.0, 0.0),
            ):
                viewer.selected_item_key = selected_key
                viewer.selected_log_row = row
            if pyimgui.is_item_clicked(1):
                pyimgui.open_popup(f"DropLogRowMenu##{row_idx}")
            if pyimgui.begin_popup(f"DropLogRowMenu##{row_idx}"):
                if pyimgui.menu_item("Identify item"):
                    viewer._identify_item_for_all_characters(parsed.item_name, rarity)
                if pyimgui.menu_item("Refresh item stats"):
                    viewer._refresh_item_stats_for_all_characters(parsed.item_name, rarity)
                pyimgui.end_popup()
            if pyimgui.is_item_hovered():
                hover_key = (
                    "log",
                    row_idx,
                    viewer._ensure_text(viewer._extract_row_event_id(row)).strip(),
                    id(row),
                )
                if _hover_preview_ready(viewer, hover_key):
                    viewer._set_hover_item_preview(selected_key, row)
                    imgui.show_tooltip(_build_item_hover_tooltip_text(viewer, row, parsed.item_name))
            pyimgui.pop_style_color(1)

            pyimgui.table_set_column_index(6)
            pyimgui.text(str(int(parsed.quantity)))

            pyimgui.table_set_column_index(7)
            pyimgui.text_colored(rarity, (r, g, b, a))

        log_scroll_to_bottom = False
        current_total = int(viewer.total_drops)
        if not viewer._log_autoscroll_initialized:
            viewer._log_autoscroll_initialized = True
        elif viewer.auto_scroll and current_total > int(viewer._last_log_autoscroll_total_drops):
            log_scroll_to_bottom = True
        viewer._last_log_autoscroll_total_drops = current_total

        if viewer._request_log_scroll_bottom:
            log_scroll_to_bottom = True
            viewer._request_log_scroll_bottom = False

        if log_scroll_to_bottom:
            pyimgui.set_scroll_here_y(1.0)

        pyimgui.end_table()
    pyimgui.pop_style_color(1)
    viewer._draw_selected_item_details()
