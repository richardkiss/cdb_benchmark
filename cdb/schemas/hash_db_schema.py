from typing import Iterable, TypeVar, Type

import pathlib
import sqlite3

# from .hash_db import HashDB, Row, HashDBProtocol

from cdb.row_array_storage import RowArrayStorage
from cdb.hashdb.sqlite3_row_array_storage import SQLite3RowStorage

from cdb.hashdb.flat_file_array_storage import FlatFileArrayStorage

# from .flat_file_db import FlatFileDB as HashDB, Row
from cdb.schema import (
    Schema,
    BlockSpendInfo,
    CoinInfo,
    Coin,
    bytes32,
    topological_sort,
    Row,
)

from cdb.hashdb.row_array_db import RowArrayDB


# Define a TypeVar for the objects
T = TypeVar("T")


COINBASE_PREFIXES = [
    bytes.fromhex(_)
    for _ in ["3ff07eb358e8255a65c30a2dce0e5fbb", "ccd5bb71183532bff220ba46c268991a"]
]

COINBASE_PREFIX_LOOKUP = {_[1]: _[0] for _ in enumerate(COINBASE_PREFIXES)}

DEBUG_COIN = bytes32.fromhex(
    "75043187b316d5f8d5a9dd8bfb26058e57db4f741e3404557b14525600685c94"
)


def list_int_to_bytes(items: list[int]) -> bytes:
    return b"".join(x.to_bytes(8, "big") for x in items)


def list_int_from_bytes(b: bytes) -> list[int]:
    return [int.from_bytes(b[i : i + 8], "big") for i in range(0, len(b), 8)]


def is_coinbase_name(coin_name: bytes32) -> bool:
    return all(_ == 0 for _ in coin_name[16:24])


def as_coinbase_index(coin_name: bytes32) -> None | int:
    if is_coinbase_name(coin_name):
        prefix_index = COINBASE_PREFIX_LOOKUP.get(coin_name[:16])
        if prefix_index is None:
            return None
        v = int.from_bytes(coin_name[16:], "big") << 8
        v |= prefix_index
        v = -v
        return v
    return None


def bytes32_for_negative_coin_index(coin_index: int) -> bytes32:
    coin_index = -coin_index
    prefix_index = coin_index & 0xFF
    prefix = COINBASE_PREFIXES[prefix_index]
    v = coin_index >> 8
    return bytes32(prefix + v.to_bytes(16, "big"))


class BaseDBSchema(Schema):
    def __init__(
        self, path: pathlib.Path, row_array_storage_class: Type[RowArrayStorage]
    ):
        path.mkdir(parents=True, exist_ok=True)

        self._row_array_db = RowArrayDB(path, row_array_storage_class)
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

    def accept_block(self, block_spend_info: BlockSpendInfo) -> None:
        self._pending_blocks.append(block_spend_info)
        self._pending_coin_count += len(block_spend_info.confirms)
        if self._pending_coin_count > self._cache_size:
            self.flush()

    def flush(self) -> None:
        unflushed_coin_lookup: dict[bytes32, int] = {}
        self._conn.execute("BEGIN TRANSACTION")
        for block in self._pending_blocks:
            self._store_block(block, unflushed_coin_lookup)
        self._flush_coin_lookup(unflushed_coin_lookup)
        self._pending_coin_count = 0
        self._pending_blocks = []
        self._conn.commit()

    def _flush_coin_lookup(self, unflushed_coin_lookup: dict[bytes32, int]) -> None:
        rows: list[Row] = list(unflushed_coin_lookup.items())
        self._row_array_db.add_rows(rows)

    def _lookup_name_id_tuples_for_coin_names(
        self, coin_names: list[bytes32], unflushed_coin_lookup: dict[bytes32, int]
    ) -> tuple[list[tuple[bytes32, int]], list[bytes32]]:
        name_id_tuples = []
        missing_coin_names = []
        for coin_name in coin_names:
            v = unflushed_coin_lookup.get(coin_name)
            if v is None:
                missing_coin_names.append(coin_name)
            else:
                name_id_tuples.append((coin_name, v))
        return name_id_tuples, missing_coin_names

    def _store_block(
        self,
        block_spend_info: BlockSpendInfo,
        unflushed_coin_lookup: dict[bytes32, int],
    ) -> None:
        cursor = self._conn.cursor()
        # first, we add all the spends to the coin table
        block_index = block_spend_info.index
        # then we add the confirms to the coin table
        # get all the parent coin ids
        confirm_ids = []

        if block_spend_info.index == 9_225698:
            breakpoint()

        coin_by_name: dict[bytes32, Coin] = {
            _.name(): _ for _ in block_spend_info.confirms
        }
        parent_names = [_.parent_coin_name for _ in block_spend_info.confirms]
        name_id_tuples, missing_coin_names = self._lookup_name_id_tuples_for_coin_names(
            parent_names, unflushed_coin_lookup
        )

        coin_ids_by_name: dict[bytes32, int] = self._fetch_coin_indices_for_coin_names(
            missing_coin_names, unflushed_coin_lookup
        )
        coin_ids_by_name.update(dict(name_id_tuples))

        sorted_confirms = topological_sort(
            set(block_spend_info.confirms),
            lambda _: [coin_by_name[_.parent_coin_name]]
            if _.parent_coin_name in coin_by_name
            else [],
        )

        for coin in sorted_confirms:
            pcn = coin.parent_coin_name
            parent_index = as_coinbase_index(pcn)
            if parent_index is None:
                parent_index = coin_ids_by_name.get(pcn)
            if parent_index is None:
                breakpoint()
                raise ValueError(f"can't find coin id for parent coin {pcn}")

            coin_name = coin.name()
            # add the coin to the coin and coin_lookup tables
            coin_amount_as_bytes = coin.amount.to_bytes(8, "big")
            cursor.execute(
                "INSERT INTO coin (parent, puzzle, amount, confirmed, spent) VALUES (?, ?, ?, ?, 0)",
                (parent_index, coin.puzzle_hash, coin_amount_as_bytes, block_index),
            )
            coin_lookup_index = cursor.lastrowid
            assert coin_lookup_index is not None
            unflushed_coin_lookup[coin_name] = coin_lookup_index
            confirm_ids.append(coin_lookup_index)
            coin_ids_by_name[coin_name] = coin_lookup_index

        spend_id_by_name = self._fetch_coin_indices_for_coin_names(
            block_spend_info.spends, unflushed_coin_lookup
        )
        spend_ids = [_[1] for _ in spend_id_by_name.items()]

        # TODO: do the `INSERT` above with the correct value
        for _ in spend_ids:
            cursor.execute("UPDATE coin SET spent=? WHERE id=?", (block_index, _))

        spends_as_blob = list_int_to_bytes(spend_ids)
        confirms_as_blob = list_int_to_bytes(confirm_ids)
        cursor.execute(
            "INSERT INTO block (id, timestamp, spends, confirms) VALUES (?, ?, ?, ?)",
            (
                block_index,
                block_spend_info.timestamp,
                spends_as_blob,
                confirms_as_blob,
            ),
        )

    def coin_infos_for_coin_names(self, coin_names: list[bytes32]) -> list[CoinInfo]:
        return []

    def block_info_for_block_index(self, block_index: int) -> None | BlockSpendInfo:
        return None

    def rewind_to_block_index(self, block_index: int) -> None:
        pass

    ###

    def _fetch_coin_indices_for_coin_names(
        self, coin_names: list[bytes32], unflushed_coin_lookup: dict[bytes32, int]
    ) -> dict[bytes32, int]:
        coin_names_remaining = []

        d = {}
        for coin_name in coin_names:
            if is_coinbase_name(coin_name):
                v = as_coinbase_index(coin_name)
            else:
                v = unflushed_coin_lookup.get(coin_name)
            if v is None:
                coin_names_remaining.append(coin_name)
            else:
                d[coin_name] = v
        # if DEBUG_COIN in coin_names_remaining:
        #    breakpoint()
        #    self._row_array_db.find_hashes([DEBUG_COIN])
        d1 = dict(self._row_array_db.find_hashes(coin_names_remaining))
        d.update(d1)
        if set(d.keys()) != set(coin_names):
            missing = set(coin_names) - set(d.keys())
            print(f"missing {missing}")
            breakpoint()

        return d

    def _coin_index_for_coin_name(
        self, coin_name: bytes32, coin_lookup: dict[bytes32, int] = {}
    ) -> int:
        if coin_name in coin_lookup:
            return coin_lookup[coin_name]
        cursor = self._conn.cursor()
        cursor.execute("SELECT id FROM coin_lookup WHERE hash=?", (coin_name,))
        return cursor.fetchone()[0]

    def _coin_name_for_coin_index(self, coin_index: int) -> bytes32:
        return self._coin_names_for_coin_indices([coin_index])[0]

    def _coin_names_for_coin_indices(self, coin_indices: list[int]) -> list[bytes32]:
        lookup: dict[int, bytes32] = {}
        pos_coin_indices = []
        for coin_index in coin_indices:
            if coin_index <= 0:
                lookup[coin_index] = bytes32_for_negative_coin_index(coin_index)
            else:
                pos_coin_indices.append(coin_index)
        cursor = self._conn.cursor()
        q_marks = ",".join("?" for _ in pos_coin_indices)
        cursor.execute(
            f"SELECT id, hash FROM coin_lookup WHERE id IN ({q_marks})",
            pos_coin_indices,
        )
        for row in cursor:
            lookup[row[0]] = row[1]
        return [lookup[_] for _ in coin_indices]

    def _coin_infos_for_coin_indices(self, coin_indices: list[int]) -> list[CoinInfo]:
        lookup: dict[int, CoinInfo] = {}
        cursor = self._conn.cursor()
        q_marks = ",".join("?" for _ in coin_indices)
        cursor.execute(
            f"SELECT id, parent, puzzle, amount, confirmed, spent FROM coin WHERE id IN ({q_marks})",
            coin_indices,
        )
        rows = list(cursor)
        parent_ids = [_[1] for _ in rows]
        parent_coin_names = self._coin_names_for_coin_indices(parent_ids)
        for row, parent_coin_name in zip(rows, parent_coin_names):
            lookup[row[0]] = CoinInfo(
                coin=Coin(
                    parent_coin_name=parent_coin_name,
                    puzzle_hash=row[2],
                    amount=int.from_bytes(row[3], "big"),
                ),
                confirmed_index=row[4],
                spent_index=row[5],
            )
        return [lookup[_] for _ in coin_indices]

    def _coins_for_coin_indices(self, coin_indices: list[int]) -> list[Coin]:
        return [_.coin for _ in self._coin_infos_for_coin_indices(coin_indices)]

    def blocks(self) -> Iterable[BlockSpendInfo]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT id, timestamp, spends, confirms FROM block")
        for row in cursor:
            block_index = row[0]
            timestamp = row[1]
            spend_ids = list_int_from_bytes(row[2])
            spends = self._coin_names_for_coin_indices(spend_ids)
            confirm_ids = list_int_from_bytes(row[3])
            confirms = self._coins_for_coin_indices(confirm_ids)
            yield BlockSpendInfo(block_index, timestamp, spends, confirms)


PATH = pathlib.Path("./hash_db_root")
# if PATH.exists():
#    raise FileExistsError(f"{PATH} already exists")

FFREPLAY = BaseDBSchema(PATH, FlatFileArrayStorage)

SQLREPLAY = BaseDBSchema(PATH, SQLite3RowStorage)
