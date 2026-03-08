import time


def trigger_selected_name_mismatch_alert(
    viewer,
    *,
    player_name: str,
    selected_item_name: str,
    row_item_name: str,
    stats_first_line: str,
    duration_s: float = 5.0,
) -> None:
    viewer.selected_name_mismatch_popup_message = (
        f"Selected item mismatch\n"
        f"Player: {viewer._ensure_text(player_name).strip() or 'Unknown'}\n"
        f"Selected: {viewer._ensure_text(selected_item_name).strip() or '-'}\n"
        f"Row: {viewer._ensure_text(row_item_name).strip() or '-'}\n"
        f"Stats: {viewer._ensure_text(stats_first_line).strip() or '-'}"
    )
    viewer.selected_name_mismatch_popup_until = time.time() + max(0.1, float(duration_s or 5.0))
    viewer.selected_name_mismatch_popup_pending = True


def begin_selected_name_mismatch_popup(viewer) -> bool:
    if bool(getattr(viewer, "selected_name_mismatch_popup_pending", False)):
        viewer.selected_name_mismatch_popup_pending = False
        return True
    return False


def selected_name_mismatch_popup_remaining(viewer) -> float:
    until_ts = float(getattr(viewer, "selected_name_mismatch_popup_until", 0.0) or 0.0)
    return max(0.0, until_ts - time.time())


def dismiss_selected_name_mismatch_popup(viewer) -> None:
    viewer.selected_name_mismatch_popup_until = 0.0
