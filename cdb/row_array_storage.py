from pathlib import Path
from typing import Iterable, Iterator

from .schema import Row

from abc import abstractmethod, ABC


class RowArrayStorage(ABC):
    @classmethod
    @abstractmethod
    def create_with_rows(
        self, file_path: Path, sorted_rows: Iterable[Row]
    ) -> "RowArrayStorage": ...

    @abstractmethod
    def __init__(self, file_path: Path): ...

    @abstractmethod
    def all_rows(self) -> Iterator[Row]: ...

    @abstractmethod
    def row_count(self) -> int: ...

    @abstractmethod
    def read_row(self, index: int) -> Row: ...

    @abstractmethod
    def requery_count(self) -> int: ...


def sorted_merged_rows(
    db_files: list[RowArrayStorage], output_db_path: Path
) -> Iterator[Row]:
    iterables = [db.all_rows() for db in db_files]
    initial_values: list[None | Row] = [next(it, None) for it in iterables]
    row_iterators: list[tuple[Row, Iterator[Row]]] = [
        (row, it) for row, it in zip(initial_values, iterables) if row is not None
    ]

    while row_iterators:
        smallest_index = min(
            (i for i, row in enumerate(row_iterators)),
            key=lambda i: row_iterators[i][0],
        )
        yield row_iterators[smallest_index][0]
        new_item = next(row_iterators[smallest_index][1], None)
        if new_item is None:
            del row_iterators[smallest_index]
        else:
            row_iterators[smallest_index] = (new_item, row_iterators[smallest_index][1])
