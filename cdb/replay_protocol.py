from dataclasses import dataclass
from typing import Protocol

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
    return bytes([size | 0x80]) + v.to_bytes(size, "big", signed=True)


@dataclass
class Coin:
    parent_coin_info: bytes32
    puzzle_hash: bytes32
    amount: int

    def name(self) -> bytes32:
        return bytes32(
            hashlib.sha256(
                self.parent_coin_info + self.puzzle_hash + as_clvm_int(self.amount)
            ).digest()
        )


@dataclass
class BlockSpendInfo:
    timestamp: int
    spends: list[bytes32]
    confirms: list[Coin]


@dataclass
class CoinInfo:
    coin: Coin
    confirmed_index: int
    spent_index: int


class Replayer(Protocol):
    def accept_block(self, block_spend_info: BlockSpendInfo) -> int:
        pass

    def coin_info_for_coin_name(self, coin_name: bytes32) -> CoinInfo:
        pass

    def block_info_for_block_index(self, block_index: int) -> BlockSpendInfo:
        pass

    def rewind_to_block_index(self, block_index: int) -> None:
        pass
