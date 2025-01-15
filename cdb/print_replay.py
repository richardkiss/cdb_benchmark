from .replay_protocol import Replayer, BlockSpendInfo, CoinInfo, bytes32


__all__ = ["PRINT_REPLAY", "PrintReplay"]


class PrintReplay(Replayer):
    def __init__(self):
        self._block_index = 0

    def accept_block(self, block_spend_info: BlockSpendInfo) -> int:
        print(block_spend_info)
        self._block_index += 1
        return self._block_index

    def coin_info_for_coin_name(self, coin_name: bytes32) -> CoinInfo:
        pass

    def block_info_for_block_index(self, block_index: int) -> BlockSpendInfo:
        pass

    def rewind_to_block_index(self, block_index: int) -> None:
        pass


PRINT_REPLAY = PrintReplay()
