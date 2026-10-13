# squeal-db, SQLite and Turso: one SQL workload

size 200000, 4 reader threads, median of 3 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 2.13 µs | 552 ns | 1.01 µs | — |
| Create 3 indexes (per index) | 160.3 ms | 35.3 ms | 86.5 ms | — |
| `ANALYZE` | 46.8 ms | 18.3 ms | 22.8 ms | — |
| Point lookup by primary key | 5.00 µs | 1.31 µs | 1.66 µs | yes |
| Secondary-index lookup + aggregate | 13.71 µs | 9.32 µs | 7.53 µs | yes |
| Index range (1 week of 52) + aggregate | 2.6 ms | 1.8 ms | 1.9 ms | yes |
| Full scan with `GROUP BY` | 30.0 ms | 44.8 ms | 39.6 ms | yes |
| Join + `GROUP BY` | 67.2 ms | 134.0 ms | 157.2 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.1 ms | 412.77 µs | 442.20 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 88.6 ms | 65.8 ms | 62.6 ms | yes |
| `IN (subquery)` | 23.4 ms | 20.8 ms | 24.6 ms | yes |
| Correlated `EXISTS` | 24.0 ms | 25.6 ms | 23.8 ms | yes |
| Correlated `NOT EXISTS` | 13.6 ms | 64.7 ms | 41.5 ms | yes |
| `UPDATE` one row, autocommit | 5.4 ms | 4.4 ms | 4.5 ms | — |
| `INSERT` one row, autocommit | 5.0 ms | 4.3 ms | 4.6 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 128.86 µs | 73.60 µs | 92.89 µs | — |
| Check: totals after the writes | 14.9 ms | 22.2 ms | 34.4 ms | yes |
| Point lookups, 4 threads (per lookup) | 2.84 µs | 1.73 µs | 1.11 µs | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 105 MiB | 35 MiB | 47 MiB |
| after reads | 108 MiB | 35 MiB | 67 MiB |
| after writes | 133 MiB | 35 MiB | 67 MiB |
| at end | 134 MiB | 46 MiB | 90 MiB |
| peak | 177 MiB | 46 MiB | 90 MiB |

## Profile `large`: 1 GiB page cache each; squeal-db also 512 MiB per query and 512 MiB of scratch, SQLite and Turso temp tables in memory

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 1024 MiB, query memory 512 MiB, scratch 512 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 2.17 µs | 519 ns | 1.02 µs | — |
| Create 3 indexes (per index) | 164.7 ms | 37.5 ms | 87.5 ms | — |
| `ANALYZE` | 46.9 ms | 16.0 ms | 24.3 ms | — |
| Point lookup by primary key | 5.01 µs | 1000 ns | 1.34 µs | yes |
| Secondary-index lookup + aggregate | 13.42 µs | 5.87 µs | 4.24 µs | yes |
| Index range (1 week of 52) + aggregate | 2.7 ms | 785.72 µs | 890.29 µs | yes |
| Full scan with `GROUP BY` | 29.6 ms | 42.2 ms | 29.8 ms | yes |
| Join + `GROUP BY` | 67.4 ms | 78.2 ms | 84.1 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.1 ms | 213.81 µs | 304.62 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 82.3 ms | 60.3 ms | 53.8 ms | yes |
| `IN (subquery)` | 23.3 ms | 9.7 ms | 12.6 ms | yes |
| Correlated `EXISTS` | 24.0 ms | 14.0 ms | 17.5 ms | yes |
| Correlated `NOT EXISTS` | 13.2 ms | 31.4 ms | 35.7 ms | yes |
| `UPDATE` one row, autocommit | 5.4 ms | 4.3 ms | 4.5 ms | — |
| `INSERT` one row, autocommit | 5.0 ms | 4.5 ms | 4.5 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 105.92 µs | 99.97 µs | 105.10 µs | — |
| Check: totals after the writes | 13.6 ms | 27.0 ms | 32.1 ms | yes |
| Point lookups, 4 threads (per lookup) | 2.87 µs | 1.58 µs | 1.02 µs | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 105 MiB | 22 MiB | 49 MiB |
| after reads | 108 MiB | 30 MiB | 58 MiB |
| after writes | 133 MiB | 31 MiB | 58 MiB |
| at end | 134 MiB | 61 MiB | 78 MiB |
| peak | 176 MiB | 61 MiB | 78 MiB |

