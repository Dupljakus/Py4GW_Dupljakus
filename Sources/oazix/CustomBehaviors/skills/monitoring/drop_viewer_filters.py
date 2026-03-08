from Sources.oazix.CustomBehaviors.skills.monitoring.drop_tracker_runtime_store import (
    rebuild_aggregates_from_runtime_rows,
)


def _rows_version(viewer) -> int:
    try:
        return max(0, int(getattr(viewer, "_runtime_rows_version", 0) or 0))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        return 0


def _filter_signature(viewer) -> tuple:
    try:
        search_text = viewer._ensure_text(getattr(viewer, "search_text", "")).strip().lower()
        filter_player = viewer._ensure_text(getattr(viewer, "filter_player", "")).strip().lower()
        filter_map = viewer._ensure_text(getattr(viewer, "filter_map", "")).strip().lower()
        min_qty = max(1, int(getattr(viewer, "min_qty", 1) or 1))
        rarity_idx = int(getattr(viewer, "filter_rarity_idx", 0) or 0)
        only_rare = bool(getattr(viewer, "only_rare", False))
        hide_gold = bool(getattr(viewer, "hide_gold", False))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        search_text = ""
        filter_player = ""
        filter_map = ""
        min_qty = 1
        rarity_idx = 0
        only_rare = False
        hide_gold = False
    return (
        search_text,
        filter_player,
        filter_map,
        min_qty,
        rarity_idx,
        only_rare,
        hide_gold,
    )


def rebuild_aggregates_from_raw_drops(viewer) -> None:
    temp_agg, total = rebuild_aggregates_from_runtime_rows(
        raw_drops=viewer.raw_drops,
        parse_runtime_row_fn=viewer._parse_drop_row,
        canonical_name_fn=viewer._canonical_agg_item_name,
        safe_int_fn=viewer._safe_int,
        ensure_text_fn=viewer._ensure_text,
    )
    viewer.aggregated_drops = temp_agg
    viewer.total_drops = int(total)


def is_rare_rarity(_viewer, rarity):
    return rarity == "Gold"


def passes_filters(viewer, row):
    parsed = viewer._parse_drop_row(row)
    if parsed is None:
        return False

    player_name = viewer._display_player_name(parsed.player_name, getattr(parsed, "sender_email", ""))
    item_name = viewer._ensure_text(parsed.item_name)
    qty = int(parsed.quantity)
    rarity = viewer._ensure_text(parsed.rarity).strip() or "Unknown"
    map_name = viewer._ensure_text(parsed.map_name)

    if qty < max(1, int(viewer.min_qty)):
        return False
    if viewer.only_rare and not viewer._is_rare_rarity(rarity):
        return False
    if viewer.hide_gold and viewer._clean_item_name(item_name) == "Gold":
        return False
    if viewer.filter_rarity_idx > 0:
        wanted = viewer.filter_rarity_options[viewer.filter_rarity_idx]
        if wanted == "Unknown":
            if "Unknown" not in rarity:
                return False
        elif rarity != wanted:
            return False

    search = viewer.search_text.strip().lower()
    if search:
        haystack = f"{item_name} {player_name} {map_name} {rarity}".lower()
        if search not in haystack:
            return False

    fp = viewer.filter_player.strip().lower()
    if fp and fp not in player_name.lower():
        return False

    fm = viewer.filter_map.strip().lower()
    if fm and fm not in map_name.lower():
        return False

    return True


def get_filtered_rows(viewer):
    cache_map = getattr(viewer, "_cached_filtered_rows", None)
    if not isinstance(cache_map, dict):
        cache_map = {}
        setattr(viewer, "_cached_filtered_rows", cache_map)

    try:
        raw_len = len(getattr(viewer, "raw_drops", []) or [])
    except (TypeError, ValueError, RuntimeError, AttributeError):
        raw_len = 0
    cache_key = (_rows_version(viewer), raw_len, _filter_signature(viewer))
    cached_key = cache_map.get("key")
    cached_rows = cache_map.get("rows")
    if cached_key == cache_key and isinstance(cached_rows, list):
        return cached_rows

    rows = [row for row in viewer.raw_drops if viewer._passes_filters(row)]
    cache_map["key"] = cache_key
    cache_map["rows"] = rows
    return rows


def is_gold_row(viewer, row):
    parsed = viewer._parse_drop_row(row)
    if parsed is None:
        return False
    return viewer._clean_item_name(parsed.item_name) == "Gold"


def get_filtered_aggregated(viewer, filtered_rows):
    cache_map = getattr(viewer, "_cached_filtered_aggregated", None)
    if not isinstance(cache_map, dict):
        cache_map = {}
        setattr(viewer, "_cached_filtered_aggregated", cache_map)
    cache_key = (
        _rows_version(viewer),
        id(filtered_rows),
        len(filtered_rows or []),
        _filter_signature(viewer),
    )
    cached_key = cache_map.get("key")
    cached_agg = cache_map.get("agg")
    cached_total = cache_map.get("total_qty")
    if cached_key == cache_key and isinstance(cached_agg, dict):
        return cached_agg, int(cached_total or 0)

    agg = {}
    total_qty = 0
    for row in filtered_rows:
        parsed = viewer._parse_drop_row(row)
        if parsed is None:
            continue
        item_name = parsed.item_name
        rarity = parsed.rarity
        qty = int(parsed.quantity)
        total_qty += qty
        canonical_name = viewer._canonical_agg_item_name(item_name, rarity, agg)
        key = (canonical_name, rarity)
        if key not in agg:
            agg[key] = {"Quantity": 0, "Count": 0}
        agg[key]["Quantity"] += qty
        agg[key]["Count"] += 1
    cache_map["key"] = cache_key
    cache_map["agg"] = agg
    cache_map["total_qty"] = int(total_qty)
    return agg, total_qty


def get_table_rows(viewer, filtered_rows, view_mode: str = ""):
    if view_mode != "Log":
        return filtered_rows

    latest_only = bool(getattr(viewer, "log_latest_n_enabled", True))
    if not latest_only:
        return filtered_rows

    try:
        latest_n = max(20, int(getattr(viewer, "log_latest_n", 200) or 200))
    except (TypeError, ValueError, RuntimeError, AttributeError):
        latest_n = 200

    if len(filtered_rows or []) <= latest_n:
        return filtered_rows

    cache_map = getattr(viewer, "_cached_table_rows", None)
    if not isinstance(cache_map, dict):
        cache_map = {}
        setattr(viewer, "_cached_table_rows", cache_map)

    cache_key = (
        _rows_version(viewer),
        id(filtered_rows),
        len(filtered_rows or []),
        view_mode,
        latest_only,
        latest_n,
    )
    cached_key = cache_map.get("key")
    cached_rows = cache_map.get("rows")
    if cached_key == cache_key and isinstance(cached_rows, list):
        return cached_rows

    rows = list(filtered_rows[-latest_n:])
    cache_map["key"] = cache_key
    cache_map["rows"] = rows
    return rows
