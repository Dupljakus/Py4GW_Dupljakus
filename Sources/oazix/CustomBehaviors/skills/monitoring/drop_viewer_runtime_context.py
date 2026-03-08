import sys

from Py4GWCoreLib import GLOBAL_CACHE, Map, Player, Py4GW

EXPECTED_RUNTIME_ERRORS = (TypeError, ValueError, RuntimeError, AttributeError, IndexError, KeyError, OSError)


def viewer_runtime_module(viewer):
    try:
        return sys.modules.get(viewer.__class__.__module__)
    except EXPECTED_RUNTIME_ERRORS:
        return None


def runtime_attr(viewer, name: str, fallback):
    module = viewer_runtime_module(viewer)
    if module is not None and hasattr(module, name):
        return getattr(module, name)
    return fallback
