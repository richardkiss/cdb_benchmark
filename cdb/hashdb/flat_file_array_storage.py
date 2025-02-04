# by ChatGPT

from pathlib import Path
from typing import Iterable, Iterator
import struct

from cdb.row_array_storage import RowArrayStorage
from cdb.schema import Row


class FlatFileDB(RowArrayStorage):
    ROW_FORMAT = ">32sQ"  # 32-byte hash (bytes32) + 8-byte uint64
    ROW_SIZE = struct.calcsize(ROW_FORMAT)

    @classmethod
    def create_with_rows(
        cls, file_path: Path, sorted_rows: Iterable[Row]
    ) -> "FlatFileDB":
        # print(f"path = {file_path}")
        with file_path.open("wb") as f:
            for row in sorted_rows:
                f.write(struct.pack(cls.ROW_FORMAT, row[0], row[1]))
        return cls(file_path)

    def __init__(self, file_path: Path):
        self._file_path = file_path
        self._row_count = self.requery_count()

    def row_count(self) -> int:
        return self._row_count

    def all_rows(self) -> Iterator[Row]:
        with self._file_path.open("rb") as f:
            while chunk := f.read(self.ROW_SIZE):
                yield struct.unpack(self.ROW_FORMAT, chunk)

    def read_row(self, index: int) -> Row:
        with self._file_path.open("rb") as f:
            f.seek(index * self.ROW_SIZE)
            chunk = f.read(self.ROW_SIZE)
            try:
                return struct.unpack(self.ROW_FORMAT, chunk)
            except struct.error:
                breakpoint()

    def requery_count(self) -> int:
        file_size = self._file_path.stat().st_size
        row_count = file_size // self.ROW_SIZE
        assert row_count * self.ROW_SIZE == file_size
        return row_count
