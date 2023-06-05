# ruff: noqa: E402, F403
__all__ = [
    "runtime",
    "Runtime",
    "data_types",
]

def __add_submodule(path, src):
    import sys
    sys.modules[path] = src

from synapse._internal import runtime, Runtime, data_types
from synapse._internal.data_types import *

__add_submodule("synapse.data_types", data_types)
