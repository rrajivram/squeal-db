# The store, SQLite and redb as key-value stores

size 2000000, 4 reader threads, median of 1 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — store: 128 MiB page cache; SQLite: ~2 MiB; redb: 1 GiB

- **store**: squeal-db's store, called directly. Running with page cache 128 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled, as a table of (key, blob)
- **redb**: redb 4.3, a pure-Rust embedded key-value store

| phase | store | sqlite | redb | same answers |
|---|---:|---:|---:|---|
| Insert, both tables, 100k rows a transaction (per row) | 2.44 µs | 603 ns | 954 ns | — |
| Checkpoint after the load | 13.9 ms | 164.12 µs | 333 ns | — |
| Point lookup, integer key | 4.56 µs | 997 ns | 383 ns | yes |
| Point lookup, a transaction each | 6.03 µs | 1.67 µs | 679 ns | yes |
| Point lookup, composite key | 7.64 µs | 1.55 µs | 462 ns | yes |
| Full scan (per row) | 51 ns | 44 ns | 24 ns | yes |
| Prefix scan of 100 rows (per scan) | 25.73 µs | 7.57 µs | 3.85 µs | yes |
| Range scan of 1% of the table (per row) | 125 ns | 52 ns | 25 ns | yes |
| Update one row and commit | 4.1 ms | 4.1 ms | 4.8 ms | — |
| Insert one row and commit | 4.1 ms | 4.0 ms | 4.5 ms | — |
| Update, 100 rows a transaction (per row) | 66.83 µs | 65.69 µs | 97.57 µs | — |
| Delete, one transaction (per row) | 5.50 µs | 693 ns | 663 ns | — |
| Check: both tables after the writes (per row) | 67 ns | 47 ns | 26 ns | yes |
| Point lookups on 4 threads (per lookup) | 1.83 µs | 1.29 µs | 110 ns | yes |

| memory | store | sqlite | redb |
|---|---:|---:|---:|
| after open | 2 MiB | 1 MiB | 2 MiB |
| after load | 661 MiB | 4 MiB | 601 MiB |
| after reads | 342 MiB | 7 MiB | 605 MiB |
| after writes | 423 MiB | 8 MiB | 608 MiB |
| at end | 423 MiB | 21 MiB | 612 MiB |
| peak | 710 MiB | 21 MiB | 612 MiB |

## Profile `large`: 1 GiB of cache each

- **store**: squeal-db's store, called directly. Running with page cache 1024 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled, as a table of (key, blob)
- **redb**: redb 4.3, a pure-Rust embedded key-value store

| phase | store | sqlite | redb | same answers |
|---|---:|---:|---:|---|
| Insert, both tables, 100k rows a transaction (per row) | 2.44 µs | 582 ns | 887 ns | — |
| Checkpoint after the load | 11.8 ms | 162.62 µs | 8.62 µs | — |
| Point lookup, integer key | 1.14 µs | 712 ns | 395 ns | yes |
| Point lookup, a transaction each | 3.26 µs | 1.29 µs | 704 ns | yes |
| Point lookup, composite key | 1.80 µs | 835 ns | 470 ns | yes |
| Full scan (per row) | 21 ns | 30 ns | 24 ns | yes |
| Prefix scan of 100 rows (per scan) | 11.14 µs | 5.54 µs | 3.92 µs | yes |
| Range scan of 1% of the table (per row) | 90 ns | 37 ns | 25 ns | yes |
| Update one row and commit | 3.5 ms | 4.1 ms | 4.8 ms | — |
| Insert one row and commit | 3.5 ms | 4.1 ms | 4.5 ms | — |
| Update, 100 rows a transaction (per row) | 54.78 µs | 67.29 µs | 99.46 µs | — |
| Delete, one transaction (per row) | 5.86 µs | 697 ns | 632 ns | — |
| Check: both tables after the writes (per row) | 23 ns | 32 ns | 26 ns | yes |
| Point lookups on 4 threads (per lookup) | 494 ns | 2.24 µs | 111 ns | yes |

| memory | store | sqlite | redb |
|---|---:|---:|---:|
| after open | 2 MiB | 1 MiB | 2 MiB |
| after load | 1671 MiB | 590 MiB | 602 MiB |
| after reads | 1671 MiB | 593 MiB | 605 MiB |
| after writes | 1680 MiB | 594 MiB | 609 MiB |
| at end | 1682 MiB | 620 MiB | 612 MiB |
| peak | 1682 MiB | 1515 MiB | 612 MiB |

