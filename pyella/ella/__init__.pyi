__all__ = [
    "types",
    "table",
    "frame",
    "lazy",
    "open",
    "connect",
    "column",
    "topic",
    "Ella",
    "now",
    "bool_",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
    "timestamp",
    "duration",
    "string",
]

import numpy
import typing as T

from ella import types, table, frame, lazy
from ella.types import (
    bool_,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
    float32,
    float64,
    timestamp,
    duration,
    string,
)

def now() -> numpy.datetime64: ...

class Ella:
    @property
    def tables(self) -> table.TableAccessor: ...
    @property
    def config(self) -> T.Mapping[str, T.Any]: ...
    def query(self, sql: str) -> lazy.Lazy: ...
    def shutdown(self) -> None: ...
    def __enter__(self) -> "Ella": ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...

def open(
    root: str,
    serve: T.Optional[str] = "127.0.0.1:50052",
    config: T.Optional[T.Mapping[str, T.Any]] = None,
    create: bool = False,
) -> Ella: ...
def connect(addr: str) -> Ella: ...
def column(
    name: str,
    data_type: types.DataType,
    required: bool = False,
    row_shape: T.Optional[T.Sequence[int]] = None,
) -> table.ColumnInfo: ...
def topic(
    *columns: table.ColumnInfo,
    temporary: bool = False,
    index: T.List[T.Tuple[str, bool]] = []
) -> table.TopicInfo: ...
