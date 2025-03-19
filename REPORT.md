# Benchmarking

Chia's coin database uses sqlite3 and has bytes32 sha256 hash primary keys on very
large tables. This causes writes to be slow, as inserting new coins causes
large sections of the btree database structure to be rewritten. Performance
degrades to impractical levels on most consumer hardware. We describe the
benchmarking code developed to evaluate more efficient schemas.

Here is the protocol all benchmarking schemas follow:

```python
@dataclass(eq=True, frozen=True)
class Coin:
    parent_coin_name: bytes32
    puzzle_hash: bytes32
    amount: int

    def name(self) -> bytes32:
        v = bytes32(
            hashlib.sha256(
                self.parent_coin_name + self.puzzle_hash + as_clvm_int(self.amount)
            ).digest()
        )
        return v


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

    def block_info_for_block_index(self, block_index: int) -> None | BlockSpendInfo: ...

    def rewind_to_block_index(self, block_index: int) -> None: ...

    def blocks(self) -> Iterable[BlockSpendInfo]: ...
```

## Replay File

A `dump_blocks` tool uses the `Schema.blocks` api to generate a text "replay file"
from list of blocks from the standard `blockchain_v2_mainnet.sqlite` file created
by Chia's full node. You can limit the count, so only the first N blocks are dumped
into the replay file.

We dump the first set of blocks from mainnet with index below 800k. Recall that
most blocks have no transactions so the count is actually substantially less
than 800k. But it's coin count that matters. The replay file is 2.1G and includes
13,423,068 coin creations and 5,840,449 coin spends.

A corresponding `load_blocks` tool reads the replay file and inserts the blocks
into a schema. This closely mirrors the operations that would need to be
performed while synching the block chain, and it is used for benchmarking.

Each benchmark is run on two machines code named SLOW and FAST.

### SLOW machine specifications

A book-sized 2016-era Intel machine: 8GB RAM; Intel(R) Celeron(R) CPU J1800 @ 2.41GHz
processor with two cores; slow 500 GB SSD drive; running debian proxmox with zfs.

```sh
# dd if=/dev/urandom of=foo bs=16k count=100000
100000+0 records in
100000+0 records out
1638400000 bytes (1.6 GB, 1.5 GiB) copied, 97.2455 s, 16.8 MB/s
```

### FAST machine specifications

Macbook Air M2 with 24GB of RAM and a standard SSD drive, which seems very fast.
```sh
% dd if=/dev/urandom of=fooo bs=16k count=100000
100000+0 records in
100000+0 records out
1638400000 bytes transferred in 3.698980 secs (442932917 bytes/sec)
```

So baseline drive write speed is 26x faster on FAST than on SLOW.


# `load_blocks` on 800k blocks

The command-line used was the following:

```
pv -bartIe ../replay-800k.txt | python3 -m cdb.cmds.load_blocks cdb.schemas.${SCHEMA}:REPLAY --max-blocks=3000000
```

We use `pv` to get live feedback on bandwidth from the replay file and estimates of
ETA to increase confidence that it hasn't died.

## Schemas Benchmarked

### Using Sqlite3 indices to lookup coins by hash

#### `blockchain_v2_mainnet.py`

This schema corresponds to the existing Chia blockchain database schema except
with unnecessary indices removed. The coins are stored in an SQLite3 table:

```sql
CREATE TABLE IF NOT EXISTS coin_record(
coin_name blob PRIMARY KEY,
 confirmed_index bigint,
 spent_index bigint,
 coinbase int,
 puzzle_hash blob,
 coin_parent blob,
 amount blob,
 timestamp bigint)
```

Using `coin_name` as the primary key causes this table to be very slow to
insert, especially as it grows in size because the btree-storage mechanism that
sqlite3 uses requires many blocks to be written for each new entry.

Removing the primary key speeds up writes immensely, but the lookups would no
longer be efficient. All other schemas are attempts to find a more efficient
way to store the coin-name-to-coin lookups.

Replaying benchmark:

- FAST: 22m 38s
- SLOW: 3242m (2.25 days)

### Alternative methods to lookup coins by hash

We use a mechanism called `hash_db_schema` to store the coins in an sqlite3 database
using the built-in sqlite3 column `rowid` as the only index. Then we try various
mechanisms to do the lookup from the bytes32 hash to the integer index. (This
look-up is the main bottleneck in the existing schema.)

#### `SCHEMA=sqlite_v3`

Coin lookups are a table:

```sql
CREATE TABLE if not exists coin_lookup (hash BLOB PRIMARY KEY, id INTEGER)
```

This isn't much more efficient than the original schema, but it's a bit faster
to write because the rows are smaller. But the write amplification is still
the same order of magnitude.

- FAST: 20m 1s
- SLOW: 1167m (19.5 hours)

### `SCHEMA=sqlite_row_schema`

Here, we store coin-lookups in a table but there is no index. Instead, we
manually enforce that the keys are written in order and use binary search.

```sql
CREATE TABLE IF NOT EXISTS hashes (
    hash BLOB,
    hash_index INTEGER
)
```

Because we can only append, instead we have multiple DBs. We cache rows in
memory, and once we have enough to flush, we write them out in sorted order.
This is a baby log-structured merge-tree (LSM tree), like the one used in RocksDB.

If the number of DBs exceeds a threshold, we merge several of them down into a
single DB. This is a canonical "sort on disk" algorithm (merging multiple
sorted files on disk is pretty straightforward if you think about it).

- FAST: 119m (2 hours)
- SLOW: 937m (15.6 hours)

### `SCHEMA=flat_file_schema`

This is similar to `sqlite_row_schema` but instead of using sqlite3, we write
the rows to a flat file. We use a binary search and merging just as we did
above. In theory, this should be faster than using sqlite3, although we have to
roll our own atomicity.

This implementation is not currently industrial strength, just a proof of concept.

- FAST: 41m 48s
- SLOW: 559m (9.3 hours)

### `SCHEMA=rocks_schema`

This schema uses RocksDB as the key-value storage to convert bytes32 hashes to
integer indices.

It includes a ChatGPT-generated cffi interface over the rocksdb library. It's
not robust or heavily tested, but it works for the purpose this benchmark.

- FAST: 4m 31s
- SLOW: 170m (2.8 hours)
- SLOW spinning drive: 116m (1.9 hours)

#### BONUS: RocksDB on spinning drive

We ran the RocksDB benchmark on a spinning drive on SLOW. Much to my surprise,
it was faster than the SSD somehow.

Raw throughput is 48.0 MB/s on the spinning drive vs 16.8 MB/s on the SSD, which
more than makes up for the seek time.

```
# echo "spinning drive"
# dd if=/dev/urandom of=foo bs=16k count=100000
100000+0 records in
100000+0 records out
1638400000 bytes (1.6 GB, 1.5 GiB) copied, 34.1439 s, 48.0 MB/s
```


## Summary

As you can see, the RocksDB schema is the fastest by far. It even helps the
FAST machine a lot by reducing write amplification and reducing wear on SSD
drives, and enabling using a cheaper spinning disk.
