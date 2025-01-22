from typing import Callable, Iterable, List, Set, TypeVar

import pathlib
import sqlite3

from .schema import Schema, BlockSpendInfo, CoinInfo, Coin, bytes32


__all__ = ["REPLAY"]

# Define a TypeVar for the objects
T = TypeVar("T")


COINBASE_PREFIXES = [
    bytes.fromhex(_)
    for _ in ["3ff07eb358e8255a65c30a2dce0e5fbb", "ccd5bb71183532bff220ba46c268991a"]
]

COINBASE_PREFIX_LOOKUP = {_[1]: _[0] for _ in enumerate(COINBASE_PREFIXES)}


def list_int_to_bytes(items: list[int]) -> bytes:
    return b"".join(x.to_bytes(8, "big") for x in items)


def list_int_from_bytes(b: bytes) -> list[int]:
    return [int.from_bytes(b[i : i + 8], "big") for i in range(0, len(b), 8)]


def as_coinbase_index(coin_name: bytes32) -> None | int:
    if all(_ == 0 for _ in coin_name[16:24]):
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


def topological_sort(
    objects: Set[T], fetch_dependencies: Callable[[T], Iterable[T]]
) -> List[T]:
    """
    Perform a topological sort on a set of objects with dependencies.

    Args:
        objects: A set of objects to sort.
        fetch_dependencies: A function that returns the dependencies of a given object.

    Returns:
        A list of objects in a valid processing order.
    """
    # Result list to store the sorted order
    result: List[T] = []
    # Set to keep track of visited nodes to avoid reprocessing
    visited: Set[T] = set()
    # Set to detect cycles (temporary marking during DFS)
    temp_marked: Set[T] = set()

    def visit(node: T) -> None:
        """
        Recursive helper function to perform DFS and add nodes to the result.
        """
        if node in temp_marked:
            raise ValueError("Cycle detected in the dependency graph")
        if node not in visited:
            temp_marked.add(node)
            # Fetch dependencies and visit them recursively
            for dependency in fetch_dependencies(node):
                visit(dependency)
            temp_marked.remove(node)
            visited.add(node)
            result.append(node)

    # Iterate over all objects and start DFS if not visited
    for obj in objects:
        if obj not in visited:
            visit(obj)

    return result


class SQLiteReplay(Schema):
    def __init__(self, path: pathlib.Path):
        self._conn = sqlite3.connect(path)
        #
        # we have two coin tables:
        #    coin_lookup: just a u64 id autoincrement and a bytes32 hash. There is an index on the bytes32
        #    coin: a u64 id, a parent hash (foreign key to coin_lookup, or negative values have special meanings);
        #      a puzzle hash; an amount; a confirm_index; and a spent_index
        #
        # We could aggregate puzzle hashes like we do coin names, but that can come later

        self._conn.execute(
            "CREATE TABLE if not exists coin_lookup (hash BLOB PRIMARY KEY, id INTEGER)"
        )
        self._conn.execute(
            "CREATE TABLE if not exists coin (id INTEGER PRIMARY KEY AUTOINCREMENT, parent INTEGER, puzzle BLOB, amount BLOB, confirmed INTEGER, spent INTEGER)"
        )
        # We have one block table:
        #   block: a u64 index; a timestamp; a list[u64] for spends (a blob); a two u64s for confirms (initial value; count)
        self._conn.execute(
            "CREATE TABLE if not exists block (id INTEGER, timestamp INTEGER, spends BLOB, confirms BLOB)"
        )

    def accept_block(self, block_spend_info: BlockSpendInfo) -> None:
        # first, we add all the spends to the coin table
        block_index = block_spend_info.index
        cursor = self._conn.cursor()
        # then we add the confirms to the coin table
        # get all the parent coin ids
        confirm_ids = []

        if block_spend_info.index == 9_225698:
            breakpoint()
        coin_by_name = {_.name(): _ for _ in block_spend_info.confirms}
        parent_names = [_.parent_coin_name for _ in block_spend_info.confirms]
        coin_ids_by_name: dict[bytes32, int] = self.fetch_coin_indices_for_coin_names(
            parent_names
        )

        sorted_confirms = topological_sort(
            set(block_spend_info.confirms),
            lambda _: [coin_by_name[_]] if _.parent_coin_name in coin_by_name else [],
        )

        for coin in sorted_confirms:
            pcn = coin.parent_coin_name
            parent_index = as_coinbase_index(pcn)
            if parent_index is None:
                parent_index = coin_ids_by_name.get(pcn)
            if parent_index is None:
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
            cursor.execute(
                "INSERT INTO coin_lookup (hash, id) VALUES (?, ?)",
                (coin_name, coin_lookup_index),
            )
            assert cursor.lastrowid == coin_lookup_index
            confirm_ids.append(coin_lookup_index)
            coin_ids_by_name[coin_name] = coin_lookup_index

        spend_ids = [
            self.coin_index_for_coin_name(_, coin_ids_by_name)
            for _ in block_spend_info.spends
        ]
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

        self._conn.commit()
        cursor.close()

    def coin_infos_for_coin_names(self, coin_names: list[bytes32]) -> list[CoinInfo]:
        pass

    def block_info_for_block_index(self, block_index: int) -> BlockSpendInfo:
        pass

    def rewind_to_block_index(self, block_index: int) -> None:
        pass

    ###

    def fetch_coin_indices_for_coin_names(
        self, coin_names: list[bytes32]
    ) -> dict[bytes32, int]:
        cursor = self._conn.cursor()
        q_marks = ",".join("?" for _ in coin_names)
        cursor.execute(
            f"SELECT hash, id FROM coin_lookup where hash in ({q_marks})", coin_names
        )
        return {_[0]: _[1] for _ in cursor}

    def coin_index_for_coin_name(
        self, coin_name: bytes32, coin_lookup: dict[bytes32, int] = {}
    ) -> int:
        if coin_name in coin_lookup:
            return coin_lookup[coin_name]
        cursor = self._conn.cursor()
        cursor.execute("SELECT id FROM coin_lookup WHERE hash=?", (coin_name,))
        return cursor.fetchone()[0]

    def coin_name_for_coin_index(self, coin_index: int) -> bytes32:
        return self.coin_names_for_coin_indices([coin_index])[0]

    def coin_names_for_coin_indices(self, coin_indices: list[int]) -> list[bytes32]:
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

    def coin_infos_for_coin_indices(self, coin_indices: list[int]) -> list[CoinInfo]:
        lookup: dict[int, CoinInfo] = {}
        cursor = self._conn.cursor()
        q_marks = ",".join("?" for _ in coin_indices)
        cursor.execute(
            f"SELECT id, parent, puzzle, amount, confirmed, spent FROM coin WHERE id IN ({q_marks})",
            coin_indices,
        )
        rows = list(cursor)
        parent_ids = [_[1] for _ in rows]
        parent_coin_names = self.coin_names_for_coin_indices(parent_ids)
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

    def coins_for_coin_indices(self, coin_indices: list[int]) -> list[Coin]:
        return [_.coin for _ in self.coin_infos_for_coin_indices(coin_indices)]

    def blocks(self) -> Iterable[BlockSpendInfo]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT id, timestamp, spends, confirms FROM block")
        for row in cursor:
            block_index = row[0]
            timestamp = row[1]
            spend_ids = list_int_from_bytes(row[2])
            spends = self.coin_names_for_coin_indices(spend_ids)
            confirm_ids = list_int_from_bytes(row[3])
            confirms = self.coins_for_coin_indices(confirm_ids)
            yield BlockSpendInfo(block_index, timestamp, spends, confirms)


PATH = pathlib.Path("./coin_db_v3.db")
# if PATH.exists():
#    raise FileExistsError(f"{PATH} already exists")

REPLAY = SQLiteReplay(PATH)
