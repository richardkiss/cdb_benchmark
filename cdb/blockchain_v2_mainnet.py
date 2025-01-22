from typing import Any, Callable, Generator, Iterable, List, Set, TypeVar

import pathlib
import sqlite3

from .schema import Schema, BlockSpendInfo, CoinInfo, Coin, bytes32


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


def coin_for_row(row: tuple[Any, ...]) -> Coin:
    return Coin(
        parent_coin_name=row[1],
        puzzle_hash=row[2],
        amount=int.from_bytes(row[3], "big"),
    )


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


def is_coinbase(coin: Coin) -> bool:
    return False


class SQLiteReplay_v2(Schema):
    def __init__(self, path: pathlib.Path):
        self._conn = sqlite3.connect(path)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS coin_record("
            "coin_name blob PRIMARY KEY,"
            " confirmed_index bigint,"
            " spent_index bigint,"  # if this is zero, it means the coin has not been spent
            " coinbase int,"
            " puzzle_hash blob,"
            " coin_parent blob,"
            " amount blob,"  # we use a blob of 8 bytes to store uint64
            " timestamp bigint)"
        )
        # self._conn.execute(
        #    "CREATE INDEX IF NOT EXISTS coin_confirmed_index on coin_record(confirmed_index)"
        # )
        # self._conn.execute(
        #    "CREATE INDEX IF NOT EXISTS coin_spent_index on coin_record(spent_index)"
        # )
        # self._conn.execute(
        #    "CREATE INDEX IF NOT EXISTS coin_puzzle_hash on coin_record(puzzle_hash)"
        # )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS coin_parent_index on coin_record(coin_parent)"
        )

    def accept_block(self, block_spend_info: BlockSpendInfo) -> None:
        cursor = self._conn.cursor()
        for coin in block_spend_info.confirms:
            coin_name = coin.name()
            cursor.execute(
                "INSERT INTO coin_record VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    coin_name,
                    block_spend_info.index,
                    0,
                    is_coinbase(coin),
                    coin.puzzle_hash,
                    coin.parent_coin_name,
                    coin.amount.to_bytes(8, "big"),
                    block_spend_info.timestamp,
                ),
            )
        for coin_name in block_spend_info.spends:
            cursor.execute(
                "UPDATE coin_record SET spent_index = ? WHERE coin_name = ?",
                (block_spend_info.index, coin_name),
            )
        self._conn.commit()
        cursor.close()

    def coin_infos_for_coin_names(self, coin_names: list[bytes32]) -> list[CoinInfo]:
        raise NotImplementedError("coin_infos_for_coin_names")

    def block_info_for_block_index(self, block_index: int) -> BlockSpendInfo:
        raise NotImplementedError("block_info_for_block_index")

    def rewind_to_block_index(self, block_index: int) -> None:
        raise NotImplementedError("rewind_to_block_index")

    def coin_records_by_confirmed_index(self) -> Generator[CoinInfo, CoinInfo, None]:
        raise NotImplementedError("coin_records_by_confirmed_index")

    def blocks(self) -> Iterable[BlockSpendInfo]:
        block_index = 0

        confirm_cursor = self._conn.cursor()
        confirm_cursor.execute(
            "SELECT confirmed_index, coin_parent, puzzle_hash, amount, timestamp FROM coin_record order by confirmed_index"
        )

        spent_cursor = self._conn.cursor()
        spent_cursor.execute(
            "SELECT coin_name, spent_index, confirmed_index FROM coin_record where spent_index > 0 order by spent_index"
        )

        confirm_coin_rows = [confirm_cursor.fetchone()]
        spent_coin_rows = [spent_cursor.fetchone()]
        while len(confirm_coin_rows) > 0:
            block_index += 1
            while True:
                row = confirm_coin_rows.pop()
                if row[0] != block_index:
                    break
                confirm_coin_rows.append(row)
                new_row = confirm_cursor.fetchone()
                if new_row is None:
                    break
                confirm_coin_rows.append(new_row)
            if len(confirm_coin_rows) == 0:
                confirm_coin_rows = [row]
                continue
            confirm_coins = [coin_for_row(_) for _ in confirm_coin_rows]
            timestamp = confirm_coin_rows[0][-1]
            confirm_coin_rows = [row]

            while True:
                row = spent_coin_rows.pop()
                if row[1] != block_index:
                    break
                spent_coin_rows.append(row)
                new_row = spent_cursor.fetchone()
                if new_row is None:
                    break
                spent_coin_rows.append(new_row)
            spent_coin_names = [_[0] for _ in spent_coin_rows]
            spent_coin_rows = [row]

            block_spend_info = BlockSpendInfo(
                index=block_index,
                timestamp=timestamp,
                spends=spent_coin_names,
                confirms=confirm_coins,
            )
            yield block_spend_info


PATH = pathlib.Path("./blockchain_v2_mainnet.sqlite")

REPLAY = SQLiteReplay_v2(PATH)
