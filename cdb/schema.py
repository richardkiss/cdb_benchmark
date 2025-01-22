from dataclasses import dataclass
from typing import Iterable, Protocol

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

    def coin_infos_for_coin_names(self, coin_name: list[bytes32]) -> list[CoinInfo]: ...

    def block_info_for_block_index(self, block_index: int) -> BlockSpendInfo: ...

    def rewind_to_block_index(self, block_index: int) -> None: ...

    def blocks(self) -> Iterable[BlockSpendInfo]: ...
