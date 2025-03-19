from typing import TextIO

import argparse
import sys


from cdb.schema import BlockSpendInfo, Schema, instantiate_schema


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


def dump_blocks(f: TextIO, module_with_schema: str, max_block_index: int) -> None:
    schema = instantiate_schema(module_with_schema)
    for block_info in schema.blocks():
        block_index = block_info.index
        if block_index > max_block_index:
            break
        print_block_replay(block_index, block_info, f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a coin schema dump.")
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=1000000000000,
        help="Maximum number of blocks to process.",
    )
    parser.add_argument(
        "module_with_schema",
        type=str,
        help="Module with `Schema` instance. foo.bar:SchemaInstance",
    )
    args = parser.parse_args()
    dump_blocks(sys.stdout, args.module_with_schema, args.max_blocks)


if __name__ == "__main__":
    main()
