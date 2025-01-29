from dataclasses import dataclass
from typing import Callable, Iterable, List, Protocol, Set, TypeVar

import hashlib


class bytes32(bytes):
    def __str__(self) -> str:
        return self.hex()

    def __repr__(self) -> str:
        return self.hex()


def as_clvm_int(v: int) -> bytes:
    if v == 0:
        return b""
    if v < 128:
        return bytes([v])
    size = 1 + v.bit_length() // 8
    return v.to_bytes(size, "big", signed=True)


@dataclass
class Coin:
    parent_coin_name: bytes32
    puzzle_hash: bytes32
    amount: int

    def __hash__(self) -> int:
        return hash(self.name())

    def name(self) -> bytes32:
        return bytes32(
            hashlib.sha256(
                self.parent_coin_name + self.puzzle_hash + as_clvm_int(self.amount)
            ).digest()
        )


@dataclass
class BlockSpendInfo:
    index: int
    timestamp: int
    spends: list[bytes32]
    confirms: list[Coin]


@dataclass
class CoinInfo:
    coin: Coin
    confirmed_index: int
    spent_index: int


class Schema(Protocol):
    def accept_block(self, block_spend_info: BlockSpendInfo) -> None: ...

    def flush(self) -> None: ...

    def coin_infos_for_coin_names(self, coin_name: list[bytes32]) -> list[CoinInfo]: ...

    def block_info_for_block_index(self, block_index: int) -> BlockSpendInfo: ...

    def rewind_to_block_index(self, block_index: int) -> None: ...

    def blocks(self) -> Iterable[BlockSpendInfo]: ...


T = TypeVar("T")


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
