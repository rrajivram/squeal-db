# squeal-db, SQLite and Turso: one SQL workload

size 1000000, 4 reader threads, median of 1 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, 100k rows a transaction (per row) | 5.11 µs | 548 ns | 1.05 µs | — |
| Create 3 indexes (per index) | 2860.8 ms | 154.7 ms | 539.5 ms | — |
| `ANALYZE` | 277.2 ms | 68.3 ms | 96.8 ms | — |
| Point lookup by primary key | 9.33 µs | 1.61 µs | 2.11 µs | yes |
| Secondary-index lookup + aggregate | 72.02 µs | 12.06 µs | 11.94 µs | yes |
| Index range (1 week of 52) + aggregate | 41.8 ms | 11.1 ms | 14.0 ms | yes |
| Full scan with `GROUP BY` | 377.1 ms | 257.8 ms | 208.3 ms | yes |
| Join + `GROUP BY` | 1304.3 ms | 915.0 ms | 1143.7 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 19.2 ms | 3.0 ms | 3.2 ms | yes |
| Sort the whole table (`ORDER BY` without an index) | 806.6 ms | 340.9 ms | 349.6 ms | yes |
| `IN (subquery)` | 161.4 ms | 144.1 ms | 189.0 ms | yes |
| Correlated `EXISTS` | 162.5 ms | 190.2 ms | 235.7 ms | yes |
| Correlated `NOT EXISTS` | 94.9 ms | 457.2 ms | 525.9 ms | yes |
| `UPDATE` one row, autocommit | 4.9 ms | 4.0 ms | 4.1 ms | — |
| `INSERT` one row, autocommit | 4.3 ms | 4.1 ms | 4.1 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 88.90 µs | 76.02 µs | 80.72 µs | — |
| Check: totals after the writes | 164.0 ms | 41.1 ms | 45.2 ms | yes |
| Point lookups, 4 threads (per lookup) | 4.27 µs | 1.33 µs | 979 ns | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 1021 MiB | 95 MiB | 194 MiB |
| after reads | 887 MiB | 98 MiB | 206 MiB |
| after writes | 894 MiB | 98 MiB | 138 MiB |
| at end | 897 MiB | 67 MiB | 164 MiB |
| peak | 1369 MiB | 140 MiB | 206 MiB |

## Profile `large`: 1 GiB page cache each; squeal-db also 512 MiB per query and 512 MiB of scratch, SQLite and Turso temp tables in memory

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 1024 MiB, query memory 512 MiB, scratch 512 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, 100k rows a transaction (per row) | 5.10 µs | 552 ns | 1.05 µs | — |
| Create 3 indexes (per index) | 2913.7 ms | 189.3 ms | 430.2 ms | — |
| `ANALYZE` | 244.9 ms | 62.9 ms | 97.2 ms | — |
| Point lookup by primary key | 8.71 µs | 1.19 µs | 1.87 µs | yes |
| Secondary-index lookup + aggregate | 27.16 µs | 7.91 µs | 6.63 µs | yes |
| Index range (1 week of 52) + aggregate | 28.2 ms | 5.0 ms | 6.1 ms | yes |
| Full scan with `GROUP BY` | 376.0 ms | 235.6 ms | 169.9 ms | yes |
| Join + `GROUP BY` | 652.9 ms | 528.4 ms | 570.2 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 8.0 ms | 1.4 ms | 2.0 ms | yes |
| Sort the whole table (`ORDER BY` without an index) | 595.8 ms | 362.4 ms | 319.1 ms | yes |
| `IN (subquery)` | 163.2 ms | 70.8 ms | 90.3 ms | yes |
| Correlated `EXISTS` | 162.1 ms | 94.2 ms | 115.9 ms | yes |
| Correlated `NOT EXISTS` | 84.0 ms | 227.1 ms | 259.8 ms | yes |
| `UPDATE` one row, autocommit | 4.8 ms | 4.1 ms | 4.0 ms | — |
| `INSERT` one row, autocommit | 4.2 ms | 4.1 ms | 4.1 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 88.94 µs | 73.92 µs | 84.02 µs | — |
| Check: totals after the writes | 132.1 ms | 37.1 ms | 47.1 ms | yes |
| Point lookups, 4 threads (per lookup) | 3.71 µs | 1.94 µs | 1.03 µs | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 1238 MiB | 192 MiB | 146 MiB |
| after reads | 1724 MiB | 111 MiB | 331 MiB |
| after writes | 1648 MiB | 111 MiB | 331 MiB |
| at end | 1648 MiB | 222 MiB | 392 MiB |
| peak | 1953 MiB | 223 MiB | 392 MiB |

