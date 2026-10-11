# squeal-db, SQLite and Turso: one SQL workload

size 1000000, 4 reader threads, median of 1 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 5.40 µs | 577 ns | 1.18 µs | — |
| Create 3 indexes (per index) | 3716.5 ms | 182.7 ms | 612.0 ms | — |
| `ANALYZE` | 311.8 ms | 71.9 ms | 101.7 ms | — |
| Point lookup by primary key | 14.76 µs | 1.67 µs | 2.41 µs | yes |
| Secondary-index lookup + aggregate | 167.20 µs | 12.22 µs | 14.11 µs | yes |
| Index range (1 week of 52) + aggregate | 51.4 ms | 16.7 ms | 20.6 ms | yes |
| Full scan with `GROUP BY` | 242.9 ms | 290.5 ms | 234.9 ms | yes |
| Join + `GROUP BY` | 755.7 ms | 1202.2 ms | 1503.9 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 68.4 ms | 3.6 ms | 4.0 ms | yes |
| Sort the whole table (`ORDER BY` without an index) | 1382.3 ms | 394.5 ms | 398.0 ms | yes |
| `IN (subquery)` | 206.7 ms | 218.5 ms | 270.6 ms | yes |
| Correlated `EXISTS` | 237.8 ms | 275.6 ms | 324.0 ms | yes |
| Correlated `NOT EXISTS` | 172.2 ms | 599.3 ms | 722.6 ms | yes |
| `UPDATE` one row, autocommit | 4.8 ms | 4.1 ms | 4.1 ms | — |
| `INSERT` one row, autocommit | 4.3 ms | 4.1 ms | 4.2 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 133.04 µs | 70.05 µs | 75.92 µs | — |
| Check: totals after the writes | 179.2 ms | 463.2 ms | 54.6 ms | yes |
| Point lookups, 4 threads (per lookup) | 9.35 µs | 2.43 µs | 1.20 µs | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 424 MiB | 95 MiB | 96 MiB |
| after reads | 395 MiB | 98 MiB | 71 MiB |
| after writes | 337 MiB | 98 MiB | 35 MiB |
| at end | 337 MiB | 65 MiB | 41 MiB |
| peak | 459 MiB | 140 MiB | 230 MiB |

## Profile `large`: 1 GiB page cache each; squeal-db also 512 MiB per query and 512 MiB of scratch, SQLite and Turso temp tables in memory

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 1024 MiB, query memory 512 MiB, scratch 512 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 5.82 µs | 556 ns | 1.13 µs | — |
| Create 3 indexes (per index) | 3554.3 ms | 254.9 ms | 683.3 ms | — |
| `ANALYZE` | 472.3 ms | 73.1 ms | 108.2 ms | — |
| Point lookup by primary key | 38.82 µs | 1.52 µs | 2.19 µs | yes |
| Secondary-index lookup + aggregate | 33.82 µs | 10.26 µs | 8.20 µs | yes |
| Index range (1 week of 52) + aggregate | 41.1 ms | 6.7 ms | 8.5 ms | yes |
| Full scan with `GROUP BY` | 239.2 ms | 354.8 ms | 195.4 ms | yes |
| Join + `GROUP BY` | 604.5 ms | 673.5 ms | 904.0 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 14.7 ms | 1.8 ms | 2.6 ms | yes |
| Sort the whole table (`ORDER BY` without an index) | 690.7 ms | 717.3 ms | 352.4 ms | yes |
| `IN (subquery)` | 224.3 ms | 92.8 ms | 122.5 ms | yes |
| Correlated `EXISTS` | 206.2 ms | 123.1 ms | 194.6 ms | yes |
| Correlated `NOT EXISTS` | 134.1 ms | 329.4 ms | 392.2 ms | yes |
| `UPDATE` one row, autocommit | 4.4 ms | 4.1 ms | 4.0 ms | — |
| `INSERT` one row, autocommit | 4.3 ms | 4.1 ms | 4.2 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 88.03 µs | 68.98 µs | 75.99 µs | — |
| Check: totals after the writes | 146.3 ms | 40.9 ms | 55.5 ms | yes |
| Point lookups, 4 threads (per lookup) | 5.86 µs | 2.50 µs | 1.07 µs | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 770 MiB | 192 MiB | 150 MiB |
| after reads | 993 MiB | 111 MiB | 335 MiB |
| after writes | 954 MiB | 111 MiB | 335 MiB |
| at end | 955 MiB | 221 MiB | 397 MiB |
| peak | 1069 MiB | 224 MiB | 398 MiB |

