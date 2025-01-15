from typing import Callable, Iterable, List, Set, TypeVar

import pathlib
import sqlite3

from .replay_protocol import Replayer, BlockSpendInfo, CoinInfo, bytes32


__all__ = ["REPLAY"]

# Define a TypeVar for the objects
T = TypeVar("T")


def list_int_to_bytes(items: list[int]) -> bytes:
    return b"".join(x.to_bytes(8, "big") for x in items)


def list_int_from_bytes(b: bytes) -> list[int]:
    return [int.from_bytes(b[i : i + 8], "big") for i in range(0, len(b), 8)]


def as_coinbase_index(coin_name: bytes32) -> None | int:
    if all(_ == 0 for _ in coin_name[16:24]):
        v = int.from_bytes(coin_name[16:], "big") << 8
        v |= coin_name[0]
        v = -v
        return v
    return None


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


class SQLiteReplay(Replayer):
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
            "CREATE TABLE coin_lookup (hash BLOB PRIMARY KEY, id INTEGER)"
        )
        self._conn.execute(
            "CREATE TABLE coin (id INTEGER PRIMARY KEY AUTOINCREMENT, parent INTEGER, puzzle BLOB, amount BLOB, confirmed INTEGER, spent INTEGER)"
        )
        # We have one block table:
        #   block: a u64 index; a timestamp; a list[u64] for spends (a blob); a two u64s for confirms (initial value; count)
        self._conn.execute(
            "CREATE TABLE block (id INTEGER, timestamp INTEGER, spends BLOB, confirms BLOB)"
        )

    def accept_block(self, block_spend_info: BlockSpendInfo) -> None:
        # first, we add all the spends to the coin table
        block_index = block_spend_info.index
        cursor = self._conn.cursor()
        # then we add the confirms to the coin table
        # get all the parent coin ids
        confirm_ids = []

        new_coin_ids_by_name: dict[bytes32, int] = {}

        coin_by_name = {_.name(): _ for _ in block_spend_info.confirms}
        sorted_confirms = topological_sort(
            set(block_spend_info.confirms),
            lambda _: [coin_by_name[_]] if _.parent_coin_name in coin_by_name else [],
        )

        for coin in sorted_confirms:
            pcn = coin.parent_coin_name
            parent_index = as_coinbase_index(pcn)
            if parent_index is None:
                if pcn in new_coin_ids_by_name:
                    parent_index = new_coin_ids_by_name[pcn]
            if parent_index is None:
                parent_index = self.coin_index_for_coin_name(pcn)
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
            new_coin_ids_by_name[coin_name] = coin_lookup_index

        if block_spend_info.index == 225698:
            breakpoint()
        spend_ids = [
            self.coin_index_for_coin_name(_, new_coin_ids_by_name)
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

    def coin_index_for_coin_name(
        self, coin_name: bytes32, coin_lookup: dict[bytes32, int] = {}
    ) -> int:
        if coin_name in coin_lookup:
            return coin_lookup[coin_name]
        cursor = self._conn.cursor()
        cursor.execute("SELECT id FROM coin_lookup WHERE hash=?", (coin_name,))
        return cursor.fetchone()[0]


PATH = pathlib.Path("./cdb_v3.db")
if PATH.exists():
    raise FileExistsError(f"{PATH} already exists")

REPLAY = SQLiteReplay(PATH)
