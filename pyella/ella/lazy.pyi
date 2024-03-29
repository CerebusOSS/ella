__all__ = ["Lazy", "LazyIter"]

from ella.frame import DataFrame

class LazyIter:
    def __iter__(self) -> "LazyIter": ...
    def __next__(self) -> DataFrame: ...

class Lazy:
    """Lazy"""

    def execute(self) -> DataFrame: ...
    def create_view(self, table: str, if_not_exists: bool = True) -> "Lazy": ...
    def __iter__(self) -> LazyIter: ...
