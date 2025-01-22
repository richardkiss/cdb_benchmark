from typing import Iterable

from .schema import Schema, BlockSpendInfo, CoinInfo, bytes32


__all__ = ["PRINT"]


class Print(Schema):
    def __init__(self):
        pass

    def accept_block(self, block_spend_info: BlockSpendInfo) -> None:
        print(block_spend_info)

    def coin_infos_for_coin_names(self, coin_name: list[bytes32]) -> list[CoinInfo]:
        return []

    def block_info_for_block_index(self, block_index: int) -> BlockSpendInfo:
        pass

    def rewind_to_block_index(self, block_index: int) -> None:
        pass

    def blocks(self) -> Iterable[BlockSpendInfo]:
        return []


PRINT = Print()
