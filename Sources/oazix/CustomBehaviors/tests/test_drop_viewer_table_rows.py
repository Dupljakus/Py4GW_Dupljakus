from Sources.oazix.CustomBehaviors.skills.monitoring import drop_viewer_filters


class _FakeViewer:
    def __init__(self) -> None:
        self._runtime_rows_version = 1
        self._cached_table_rows = {}
        self.log_latest_n_enabled = True
        self.log_latest_n = 3


def test_get_table_rows_non_log_mode_returns_original_rows():
    viewer = _FakeViewer()
    rows = [1, 2, 3, 4, 5]

    table_rows = drop_viewer_filters.get_table_rows(viewer, rows, view_mode="Aggregated")

    assert table_rows is rows


def test_get_table_rows_log_mode_applies_latest_limit_and_uses_cache():
    viewer = _FakeViewer()
    rows = list(range(1, 31))

    first = drop_viewer_filters.get_table_rows(viewer, rows, view_mode="Log")
    second = drop_viewer_filters.get_table_rows(viewer, rows, view_mode="Log")

    assert first == list(range(11, 31))
    assert first is second


def test_get_table_rows_log_mode_disabled_returns_original_rows():
    viewer = _FakeViewer()
    viewer.log_latest_n_enabled = False
    rows = [1, 2, 3, 4, 5]

    table_rows = drop_viewer_filters.get_table_rows(viewer, rows, view_mode="Log")

    assert table_rows is rows
