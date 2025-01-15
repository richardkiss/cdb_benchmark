from typing import Iterable, TextIO

import argparse
import importlib
import sys

from .replay_protocol import BlockSpendInfo, Coin, Replayer, bytes32

"""
B block_index timestamp spend_count confirm_count
S (spend hex)
...
C (parent hex) (puzzle hex) amount
...
"""


def parse_blocks(f: TextIO) -> Iterable[tuple[int, BlockSpendInfo]]:
    while True:
        line = f.readline()
        if not line:
            break
        parts = line.strip().split()
        if parts[0] == "B":
            block_index = int(parts[1])
            timestamp = int(parts[2])
            spend_count = int(parts[3])
            confirm_count = int(parts[4])
            spends = []
            confirms = []
            for _ in range(spend_count):
                line = f.readline()
                parts = line.strip().split(" ")
                assert parts[0] == "S"
                spends.append(bytes32.fromhex(parts[1]))
            for _ in range(confirm_count):
                line = f.readline()
                parts = line.strip().split(" ")
                assert parts[0] == "C"
                parent_coin_info = bytes32.fromhex(parts[1])
                puzzle_hash = bytes32.fromhex(parts[2])
                amount = int(parts[3])
                confirms.append(Coin(parent_coin_info, puzzle_hash, amount))
            block_spend_info = BlockSpendInfo(
                block_index, timestamp=timestamp, spends=spends, confirms=confirms
            )
            yield block_index, block_spend_info
        else:
            raise ValueError(f"Unexpected line: {line}")


def instantiate_replayer(module_with_replayer: str) -> Replayer:
    module_name, _class = module_with_replayer.split(":")

    module = importlib.import_module(module_name)
    v = getattr(module, _class)
    return v


def benchmark_cb_replay(
    f: TextIO, module_with_replayer: str, max_block_index: int
) -> None:
    replayer = instantiate_replayer(module_with_replayer)
    last_block_index = 0
    for block_index, block_spend_info in parse_blocks(f):
        if block_index > max_block_index:
            break
        if last_block_index // 1000 < block_index // 1000:
            print(f"accepted block {block_index}")
        replayer.accept_block(block_spend_info)
        last_block_index = block_index


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark a cb-replay.")
    parser.add_argument(
        "-i",
        "--input",
        type=argparse.FileType("r"),
        default=sys.stdin,
        help="Input file",
    )
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=300000,
        help="Maximum number of blocks to process.",
    )
    parser.add_argument(
        "module_with_replayer",
        type=str,
        help="Module with Replayer. foo.bar:ReplayClass",
    )

    args = parser.parse_args()

    benchmark_cb_replay(args.input, args.module_with_replayer, args.max_blocks)


if __name__ == "__main__":
    main()
