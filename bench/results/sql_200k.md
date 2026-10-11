# squeal-db, SQLite and Turso: one SQL workload

size 200000, 4 reader threads, median of 3 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, 100k rows a transaction (per row) | 4.96 µs | 569 ns | 1.07 µs | — |
| Create 3 indexes (per index) | 518.2 ms | 34.0 ms | 82.4 ms | — |
| `ANALYZE` | 50.0 ms | 17.0 ms | 22.1 ms | — |
| Point lookup by primary key | 7.71 µs | 1.30 µs | 1.69 µs | yes |
| Secondary-index lookup + aggregate | 20.66 µs | 9.51 µs | 7.92 µs | yes |
| Index range (1 week of 52) + aggregate | 3.7 ms | 1.8 ms | 1.9 ms | yes |
| Full scan with `GROUP BY` | 66.3 ms | 45.5 ms | 40.1 ms | yes |
| Join + `GROUP BY` | 117.4 ms | 135.4 ms | 160.2 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.3 ms | 418.38 µs | 446.73 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 94.0 ms | 64.0 ms | 66.0 ms | yes |
| `IN (subquery)` | 31.6 ms | 20.9 ms | 24.9 ms | yes |
| Correlated `EXISTS` | 28.4 ms | 25.9 ms | 24.3 ms | yes |
| Correlated `NOT EXISTS` | 14.9 ms | 65.6 ms | 42.4 ms | yes |
| `UPDATE` one row, autocommit | 4.6 ms | 4.0 ms | 4.0 ms | — |
| `INSERT` one row, autocommit | 4.2 ms | 4.0 ms | 4.1 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 89.97 µs | 79.10 µs | 77.16 µs | — |
| Check: totals after the writes | 30.1 ms | 13.5 ms | 12.7 ms | yes |
| Point lookups, 4 threads (per lookup) | 3.67 µs | 1.41 µs | 812 ns | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 316 MiB | 35 MiB | 42 MiB |
| after reads | 320 MiB | 35 MiB | 55 MiB |
| after writes | 321 MiB | 35 MiB | 55 MiB |
| at end | 322 MiB | 46 MiB | 78 MiB |
| peak | 426 MiB | 46 MiB | 78 MiB |

## Profile `large`: 1 GiB page cache each; squeal-db also 512 MiB per query and 512 MiB of scratch, SQLite and Turso temp tables in memory

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 1024 MiB, query memory 512 MiB, scratch 512 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, 100k rows a transaction (per row) | 4.97 µs | 566 ns | 1.06 µs | — |
| Create 3 indexes (per index) | 511.4 ms | 35.7 ms | 81.4 ms | — |
| `ANALYZE` | 49.3 ms | 15.8 ms | 22.1 ms | — |
| Point lookup by primary key | 7.75 µs | 1.02 µs | 1.43 µs | yes |
| Secondary-index lookup + aggregate | 19.86 µs | 6.01 µs | 4.36 µs | yes |
| Index range (1 week of 52) + aggregate | 3.7 ms | 795.79 µs | 930.96 µs | yes |
| Full scan with `GROUP BY` | 65.5 ms | 43.9 ms | 31.2 ms | yes |
| Join + `GROUP BY` | 115.9 ms | 80.4 ms | 86.6 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.3 ms | 215.01 µs | 298.24 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 92.6 ms | 60.1 ms | 55.8 ms | yes |
| `IN (subquery)` | 31.5 ms | 9.8 ms | 12.6 ms | yes |
| Correlated `EXISTS` | 28.5 ms | 14.1 ms | 17.7 ms | yes |
| Correlated `NOT EXISTS` | 14.8 ms | 32.1 ms | 36.9 ms | yes |
| `UPDATE` one row, autocommit | 4.7 ms | 4.0 ms | 3.2 ms | — |
| `INSERT` one row, autocommit | 4.2 ms | 4.1 ms | 3.4 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 85.08 µs | 73.29 µs | 63.79 µs | — |
| Check: totals after the writes | 28.9 ms | 13.9 ms | 13.6 ms | yes |
| Point lookups, 4 threads (per lookup) | 3.76 µs | 1.39 µs | 821 ns | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 329 MiB | 22 MiB | 45 MiB |
| after reads | 321 MiB | 30 MiB | 53 MiB |
| after writes | 323 MiB | 31 MiB | 53 MiB |
| at end | 323 MiB | 61 MiB | 73 MiB |
| peak | 420 MiB | 61 MiB | 73 MiB |

