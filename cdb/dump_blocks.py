from typing import TextIO

import argparse
import importlib
import sys


from .replay_protocol import BlockSpendInfo, Replayer


def instantiate_replayer(module_with_replayer: str) -> Replayer:
    module_name, _class = module_with_replayer.split(":")

    module = importlib.import_module(module_name)
    v = getattr(module, _class)
    return v


def print_block_replay(
    block_index: int, block_spend_info: BlockSpendInfo, f: TextIO
) -> None:
    """
    B block_index timestamp spend_count confirm_count
    S (spend hex)
    ...
    C (parent hex) (puzzle hex) amount
    ...
    """
    print(
        f"B {block_index} {block_spend_info.timestamp} "
        f"{len(block_spend_info.spends)} {len(block_spend_info.confirms)}",
        file=f,
    )
    spends = sorted(block_spend_info.spends)
    for spend in spends:
        print(f"S {spend.hex()}", file=f)
    confirms = sorted(
        block_spend_info.confirms,
        key=lambda c: (c.parent_coin_name, c.puzzle_hash, c.amount),
    )
    for confirm in confirms:
        print(
            f"C {confirm.parent_coin_name.hex()} {confirm.puzzle_hash.hex()} {confirm.amount}",
            file=f,
        )


def dump_replay(f: TextIO, module_with_replayer: str, max_block_index: int) -> None:
    replayer = instantiate_replayer(module_with_replayer)
    for block_info in replayer.blocks():
        block_index = block_info.index
        if block_index > max_block_index:
            break
        print_block_replay(block_index, block_info, f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build cb-replay file.")
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=1000000000000,
        help="Maximum number of blocks to process.",
    )
    parser.add_argument(
        "module_with_replayer",
        type=str,
        help="Module with Replayer. foo.bar:ReplayClass",
    )
    args = parser.parse_args()
    dump_replay(sys.stdout, args.module_with_replayer, args.max_blocks)


if __name__ == "__main__":
    main()
