from typing import Iterable, TypeVar

import pathlib
import sqlite3

from cdb.schema import (
    BlockSpendInfo,
    bytes32,
    Row,
)

from cdb.rocks.rocksdb import RocksDB
from cdb.schemas.hash_db_schema import BaseDBSchema

__all__ = ["REPLAY"]

# Define a TypeVar for the objects
T = TypeVar("T")


class RocksDBSchema(BaseDBSchema):
    def __init__(self, path: pathlib.Path):
        path.mkdir(parents=True, exist_ok=True)

        self._path = path

        self._sql_db_path = path / "hash_db_schema.db"
        self._conn = sqlite3.connect(self._sql_db_path)
        #
        # we have one coin table:
        #    coin: a u64 id, a parent hash (foreign key to coin_lookup, or negative values have special meanings);
        #      a puzzle hash; an amount; a confirm_index; and a spent_index
        #
        # We could aggregate puzzle hashes like we do coin names, but that can come later

        self._conn.execute(
            "CREATE TABLE if not exists coin (id INTEGER PRIMARY KEY AUTOINCREMENT, parent INTEGER, puzzle BLOB, amount BLOB, confirmed INTEGER, spent INTEGER)"
        )
        # We have one block table:
        #   block: a u64 index; a timestamp; a list[u64] for spends (a blob); a two u64s for confirms (initial value; count)
        self._conn.execute(
            "CREATE TABLE if not exists block (id INTEGER, timestamp INTEGER, spends BLOB, confirms BLOB)"
        )

        # coin lookup table is the HashDB

        self._pending_blocks: list[BlockSpendInfo] = []
        self._pending_coin_count = 0
        self._cache_size = 50000

        self._row_array_db = RocksHashDB(path)


class RocksHashDB:
    def __init__(self, path: pathlib.Path):
        self._path = path
        self._rocks_db = RocksDB(path / "rocks_db")

    def add_rows(self, rows: list[Row]) -> None:
        for row in rows:
            k, v = row
            v_blob = v.to_bytes(8, "big")
            # if k.hex() == "dce550a4341e5ec31c7e3fe5c6ab9801c66ed02689725939537d8d4492465800":
            #    breakpoint()
            self._rocks_db.put(k, v_blob)
            # r1 = self._rocks_db.get(k)
            # if r1 != v_blob:
            #    breakpoint()
            #    r1 = self._rocks_db.get(k)
            # assert r1 == v_blob

    def find_hashes(self, hs: list[bytes32]) -> list[Row]:
        r = []
        if len(hs) == 0:
            return r
        for h in hs:
            v = self._rocks_db.get(h)
            if v is None:
                continue
            v_int = int.from_bytes(v, "big")
            r.append((h, v_int))
        return r


PATH = pathlib.Path("rocks_schema_db")
REPLAY = RocksDBSchema(PATH)
