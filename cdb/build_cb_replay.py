from dataclasses import dataclass
from typing import Any, Protocol, TextIO

import argparse
import hashlib
import sqlite3
import sys


from .replay_protocol import BlockSpendInfo, Coin

"""
B block_index timestamp spend_count confirm_count
S (spend hex)
...
C (parent hex) (puzzle hex) amount
...
"""


def print_block_replay(
    block_index: int, block_spend_info: BlockSpendInfo, f: TextIO
) -> None:
    print(
        f"B {block_index} {block_spend_info.timestamp} "
        f"{len(block_spend_info.spends)} {len(block_spend_info.confirms)}",
        file=f,
    )
    for spend in block_spend_info.spends:
        print(f"S {spend.hex()}", file=f)
    for confirm in block_spend_info.confirms:
        print(
            f"C {confirm.parent_coin_info.hex()} {confirm.puzzle_hash.hex()} {confirm.amount}",
            file=f,
        )


def coin_for_row(row: tuple[Any, ...]) -> Coin:
    return Coin(
        parent_coin_info=row[1],
        puzzle_hash=row[2],
        amount=int.from_bytes(row[3], "big"),
    )


def build_cb_replay(source_db: str, max_block_index: int) -> None:
    f = sys.stdout
    source_conn = sqlite3.connect(source_db)

    confirm_cursor = source_conn.cursor()
    confirm_cursor.execute(
        "SELECT confirmed_index, coin_parent, puzzle_hash, amount, timestamp FROM coin_record order by confirmed_index"
    )

    spent_cursor = source_conn.cursor()
    spent_cursor.execute(
        "SELECT coin_name, spent_index, confirmed_index FROM coin_record where spent_index > 0 order by spent_index"
    )

    block_index = 1

    confirm_coin_rows = []
    spent_coin_rows = [spent_cursor.fetchone()]
    while block_index < max_block_index:
        while True:
            row = confirm_cursor.fetchone()
            if row[0] != block_index:
                break
            confirm_coin_rows.append(row)
        confirm_coins = [coin_for_row(_) for _ in confirm_coin_rows]
        timestamp = confirm_coin_rows[0][-1]
        confirm_coin_rows = [row]

        row = spent_coin_rows.pop()
        while True:
            if row[1] != block_index:
                break
            spent_coin_rows.append(row)
            row = spent_cursor.fetchone()
        spent_coin_names = [_[0] for _ in spent_coin_rows]
        spent_coin_rows = [row]

        block_spend_info = BlockSpendInfo(
            timestamp=timestamp,
            spends=spent_coin_names,
            confirms=confirm_coins,
        )
        print_block_replay(block_index, block_spend_info, f)
        block_index += 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build cb-replay file from a SQLite database."
    )
    parser.add_argument(
        "source_db", type=str, help="Path to the source SQLite database."
    )
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=300000,
        help="Maximum number of blocks to process.",
    )

    args = parser.parse_args()

    build_cb_replay(args.source_db, args.max_blocks)


if __name__ == "__main__":
    main()
