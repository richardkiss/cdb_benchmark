from pathlib import Path
from typing import Iterable, Iterator

import sqlite3

# from .flat_file_db import FlatFileDB as DBFile, merge_dbs
from cdb.schema import bytes32
from cdb.row_array_storage import RowArrayStorage


Row = tuple[bytes32, int]


class SQLite3RowStorage(RowArrayStorage):
    @classmethod
    def create_with_rows(
        cls, file_path: Path, rows: Iterable[Row]
    ) -> "SQLite3RowStorage":
        conn = sqlite3.connect(file_path)
        # breakpoint()
        conn.execute("PRAGMA synchronouse = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        # conn.execute("PRAGMA page_size = 131072")
        cursor = conn.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS hashes (
                    hash BLOB,
                    hash_index INTEGER
                )
                """
        )
        # cursor.execute("CREATE INDEX IF NOT EXISTS hash_blob_index ON hashes (hash)")
        cursor.executemany("INSERT INTO hashes VALUES (?, ?)", rows)
        conn.commit()
        return cls(file_path)

    def __init__(self, file_path: Path):
        self._conn = sqlite3.connect(file_path)
        self._row_count = self._fetch_row_count()

    def _fetch_row_count(self):
        cursor = self._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hashes")
        return cursor.fetchone()[0]

    def row_count(self) -> int:
        return self._row_count

    def read_row(self, index: int) -> Row:
        one_based_index = index + 1
        cursor = self._conn.cursor()
        cursor.execute("SELECT hash, hash_index FROM hashes WHERE rowid = ?", (one_based_index,))
        return cursor.fetchone()

    def requery_count(self) -> int:
        cursor = self._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hashes")
        return cursor.fetchone()[0]

    def all_rows(self) -> Iterator[Row]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT hash, hash_index FROM hashes")
        return cursor

t = SQLite3RowStorage.create_with_rows(Path("test.db"), [])
