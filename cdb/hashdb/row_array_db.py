from pathlib import Path
from typing import Iterator, List, Optional, Tuple, Type

import os

from cdb.row_array_storage import RowArrayStorage, sorted_merged_rows

from cdb.schema import bytes32, Row


DEBUG_COIN = (
    bytes32.fromhex("75043187b316d5f8d5a9dd8bfb26058e57db4f741e3404557b14525600685c94")
    + b"\x00"
)


def merge_dbs(db_files: List[RowArrayStorage], output_db) -> RowArrayStorage:
    iterable_list: list[Iterator[Row]] = []
    item_list: list[Row] = []
    for db in db_files:
        try:
            iterable = db.all_rows()
            item_list.append(next(iterable))
            iterable_list.append(iterable)
        except StopIteration:
            pass

    cursor = output_db._conn.cursor()
    row_count = 0
    while item_list:
        smallest = min(range(len(item_list)), key=lambda x: item_list[x][0])
        cursor.execute("INSERT INTO hashes VALUES (?, ?)", item_list[smallest])
        row_count += 1
        try:
            item_list[smallest] = next(iterable_list[smallest])
        except StopIteration:
            del item_list[smallest]
            del iterable_list[smallest]
    output_db._conn.commit()
    output_db._row_count = row_count
    return output_db


def _find_hash_inner(
    self: RowArrayStorage, h: bytes32, lower_bound: int, upper_bound: int
) -> Optional[Row]:
    # we use binary search and the fact that rows are ordered by hash
    while True:
        if lower_bound >= upper_bound:
            return None
        mid = (upper_bound + lower_bound) // 2
        row = self.read_row(mid)
        if row is None:
            breakpoint()
            row = self.read_row(mid)
            return None
        if row[0] == h:
            return (bytes32(row[0]), row[1])
        elif row[0] < h:
            lower_bound = mid + 1
        else:
            upper_bound = mid


def _find_hashes_inner_bs(
    self: RowArrayStorage,
    hs: list[bytes32],
    lower_bound: int,  # inclusive
    upper_bound: int,  # not incluisve
    so_far: list[Row],
    missing: set[bytes32],
) -> None:
    while True:
        if lower_bound >= upper_bound:
            if DEBUG_COIN in hs:
                breakpoint()
            missing.update(hs)
            return
        if len(hs) == 1:
            r = _find_hash_inner(self, hs[0], lower_bound, upper_bound)
            if r is None:
                missing.add(hs[0])
            else:
                so_far.append(r)
            return
        mid = (upper_bound + lower_bound) // 2
        row = self.read_row(mid)
        if row is None:
            breakpoint()
            row = self.read_row(mid)
        below = []
        above = []
        for h in hs:
            if row[0] == h:
                so_far.append((bytes32(row[0]), row[1]))
            elif row[0] > h:
                below.append(h)
            else:
                above.append(h)
        if len(below) > 0:
            _find_hashes_inner_bs(self, below, lower_bound, mid, so_far, missing)
        # do the `above` ones in the next iteration
        if len(above) == 0:
            return
        hs = above
        lower_bound = mid + 1


def find_hashes(
    self: RowArrayStorage, hs: list[bytes32]
) -> tuple[list[Row], set[bytes32]]:
    # we use binary search and the fact that rows are ordered by hash
    acc: list[Row] = []
    # return self._find_hashes_inner(hs) #, self._row_count, 0, acc)
    missing: set[bytes32] = set()
    _find_hashes_inner_bs(self, hs, 0, self.row_count(), acc, missing)
    return acc, missing


class RowArrayDB:
    """
    This creates a family of row array databases in a directory. Each database
    has a table of order hashes and their indices.

    Each database is designed to be write-once of the rows in order. They are named
    `hashdb-1.db`, `hashdb-2.db`, `hashdb-3.db`, etc.

    Row array DBs can be merged under certain circumstances. The idea is that the number
    of "non-maximal" databases should increase logarithmically with the number of rows.

    When the number of non-maximals DBs exceeds the goal, the smallest pair of DBs
    can be merged together to create a new DB with the combined rows.

    Eventually when the DBs get too large, the merging time will increase too much.
    So there will be a maximum number of rows in a DB, and when a DB exceeds this, it
    will no longer be considered a candidate for merging (and no longer contributes to
    the count of DBs).
    """

    def __init__(
        self,
        dir_path: Path,
        row_array_storage_class: Type[RowArrayStorage],
        prefix="hashdb-",
    ):
        self._dir_path = dir_path
        self._prefix = prefix
        db_paths = dir_path.glob(f"{prefix}[0-9][0-9][0-9][0-9][0-9][0-9].db")
        full_db_paths = [self._dir_path / p for p in db_paths]
        self._row_array_storage_class = row_array_storage_class
        self._db_files = {p: self._row_array_storage_class(p) for p in full_db_paths}
        self._row_count = sum(db.row_count() for db in self._db_files.values())

    def row_count(self) -> int:
        return self._row_count

    def new_db_name(self) -> Path:
        index = 0
        while True:
            index += 1
            new_path = self._dir_path / f"{self._prefix}{index:06}.db"
            if new_path not in self._db_files:
                break
        return new_path

    def add_rows(self, rows: list[Row]) -> None:
        rows.sort(key=lambda x: x[0])
        new_db_name = self.new_db_name()
        db_file = self._row_array_storage_class.create_with_rows(new_db_name, rows)
        self._db_files[new_db_name] = db_file
        self._row_count += len(rows)
        self.merge()
        if self._row_count != sum(db.row_count() for db in self._db_files.values()):
            print(f"row_count = {self._row_count}")
            for db in self._db_files.values():
                print(f"db.row_count() = {db.row_count()}")
            print(
                f"sum(db.row_count() for db in self._db_files.values()) = {sum(db.row_count() for db in self._db_files.values())}"
            )
            breakpoint()
        assert self._row_count == sum(db.row_count() for db in self._db_files.values())

    def find_hashes(self, hs: List[bytes32]) -> list[Row]:
        acc: dict[bytes32, int] = {}
        pending: list[bytes32] = hs
        for db in self._db_files.values():
            if len(pending) == 0:
                break
            found, missing = find_hashes(db, pending)
            acc.update(found)
            pending = list(missing)
        if len(pending) > 0:
            breakpoint()
        return [(h, acc[h]) for h in hs]

    def _select_db_files_to_merge(self) -> List[Tuple[Path, RowArrayStorage]]:
        """
        Select the smallest pair of DBs to merge.
        """
        if len(self._db_files) < 10:
            return []
        path_db_list = sorted(self._db_files.items(), key=lambda x: x[1].row_count())[
            :2
        ]
        return path_db_list

    def merge(self) -> None:
        path_db_list = self._select_db_files_to_merge()
        if not path_db_list:
            return

        merged_db_path = self.new_db_name()
        print(f"Merging {path_db_list[0][0]} and {path_db_list[1][0]} into {merged_db_path}")
        db_list = [_[1] for _ in path_db_list]
        size = sum(1 for _ in sorted_merged_rows(db_list, merged_db_path))
        if size != sum(db.row_count() for db in db_list):
            breakpoint()
        #assert size == sum(db.row_count() for db in db_list)
        merged_db = self._row_array_storage_class.create_with_rows(
            merged_db_path, sorted_merged_rows(db_list, merged_db_path)
        )

        # Remove old DBs and add merged DB
        for path, _ in path_db_list:
            os.remove(path)
            del self._db_files[path]
        self._db_files[merged_db_path] = merged_db

        # check the counts
        for _ in self._db_files.values():
            actual_count = _.requery_count()
            assert actual_count == _.row_count()
