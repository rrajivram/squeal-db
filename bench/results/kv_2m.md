# The store, SQLite and redb as key-value stores

size 2000000, 4 reader threads, median of 1 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — store: 128 MiB page cache; SQLite: ~2 MiB; redb: 1 GiB

- **store**: squeal-db's store, called directly. Running with page cache 128 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled, as a table of (key, blob)
- **redb**: redb 4.3, a pure-Rust embedded key-value store

| phase | store | sqlite | redb | same answers |
|---|---:|---:|---:|---|
| Insert, both tables, one transaction each (per row) | 3.13 µs | 706 ns | 763 ns | — |
| Checkpoint after the load | 26.4 ms | 3.6 ms | 250 ns | — |
| Point lookup, integer key | 7.70 µs | 2.06 µs | 504 ns | yes |
| Point lookup, a transaction each | 8.75 µs | 2.01 µs | 859 ns | yes |
| Point lookup, composite key | 12.53 µs | 2.19 µs | 597 ns | yes |
| Full scan (per row) | 57 ns | 50 ns | 28 ns | yes |
| Prefix scan of 100 rows (per scan) | 69.57 µs | 8.73 µs | 4.57 µs | yes |
| Range scan of 1% of the table (per row) | 220 ns | 60 ns | 30 ns | yes |
| Update one row and commit | 4.0 ms | 4.1 ms | 4.2 ms | — |
| Insert one row and commit | 4.2 ms | 4.1 ms | 4.4 ms | — |
| Update, 100 rows a transaction (per row) | 72.20 µs | 70.38 µs | 89.97 µs | — |
| Delete, one transaction (per row) | 5.86 µs | 646 ns | 710 ns | — |
| Check: both tables after the writes (per row) | 75 ns | 52 ns | 30 ns | yes |
| Point lookups on 4 threads (per lookup) | 3.32 µs | 1.79 µs | 132 ns | yes |

| memory | store | sqlite | redb |
|---|---:|---:|---:|
| after open | 2 MiB | 1 MiB | 2 MiB |
| after load | 195 MiB | 4 MiB | 618 MiB |
| after reads | 250 MiB | 7 MiB | 619 MiB |
| after writes | 272 MiB | 8 MiB | 623 MiB |
| at end | 273 MiB | 21 MiB | 626 MiB |
| peak | 273 MiB | 21 MiB | 626 MiB |

## Profile `large`: 1 GiB of cache each

- **store**: squeal-db's store, called directly. Running with page cache 1024 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled, as a table of (key, blob)
- **redb**: redb 4.3, a pure-Rust embedded key-value store

| phase | store | sqlite | redb | same answers |
|---|---:|---:|---:|---|
| Insert, both tables, one transaction each (per row) | 3.18 µs | 561 ns | 558 ns | — |
| Checkpoint after the load | 26.9 ms | 3.0 ms | 84 ns | — |
| Point lookup, integer key | 1.40 µs | 828 ns | 532 ns | yes |
| Point lookup, a transaction each | 3.86 µs | 1.62 µs | 841 ns | yes |
| Point lookup, composite key | 2.57 µs | 1.13 µs | 525 ns | yes |
| Full scan (per row) | 22 ns | 33 ns | 28 ns | yes |
| Prefix scan of 100 rows (per scan) | 13.35 µs | 6.10 µs | 4.78 µs | yes |
| Range scan of 1% of the table (per row) | 102 ns | 42 ns | 30 ns | yes |
| Update one row and commit | 4.3 ms | 4.2 ms | 4.3 ms | — |
| Insert one row and commit | 4.0 ms | 4.0 ms | 4.3 ms | — |
| Update, 100 rows a transaction (per row) | 69.89 µs | 68.35 µs | 97.18 µs | — |
| Delete, one transaction (per row) | 18.95 µs | 691 ns | 1.44 µs | — |
| Check: both tables after the writes (per row) | 62 ns | 33 ns | 95 ns | yes |
| Point lookups on 4 threads (per lookup) | 643 ns | 4.95 µs | 1.09 µs | yes |

| memory | store | sqlite | redb |
|---|---:|---:|---:|
| after open | 2 MiB | 1 MiB | 2 MiB |
| after load | 833 MiB | 590 MiB | 618 MiB |
| after reads | 836 MiB | 592 MiB | 620 MiB |
| after writes | 848 MiB | 593 MiB | 624 MiB |
| at end | 849 MiB | 615 MiB | 626 MiB |
| peak | 849 MiB | 1509 MiB | 626 MiB |

