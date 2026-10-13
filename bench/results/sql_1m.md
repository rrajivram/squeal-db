# squeal-db, SQLite and Turso: one SQL workload

size 1000000, 4 reader threads, median of 1 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 2.21 µs | 490 ns | 992 ns | — |
| Create 3 indexes (per index) | 744.0 ms | 157.2 ms | 531.8 ms | — |
| `ANALYZE` | 251.5 ms | 67.3 ms | 97.1 ms | — |
| Point lookup by primary key | 5.96 µs | 1.67 µs | 2.02 µs | yes |
| Secondary-index lookup + aggregate | 43.25 µs | 12.08 µs | 11.51 µs | yes |
| Index range (1 week of 52) + aggregate | 42.2 ms | 11.0 ms | 13.7 ms | yes |
| Full scan with `GROUP BY` | 148.6 ms | 251.2 ms | 206.6 ms | yes |
| Join + `GROUP BY` | 326.4 ms | 892.6 ms | 1124.2 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 6.6 ms | 2.9 ms | 3.0 ms | yes |
| Sort the whole table (`ORDER BY` without an index) | 874.0 ms | 340.4 ms | 344.1 ms | yes |
| `IN (subquery)` | 124.5 ms | 143.9 ms | 184.4 ms | yes |
| Correlated `EXISTS` | 132.9 ms | 186.7 ms | 228.8 ms | yes |
| Correlated `NOT EXISTS` | 72.7 ms | 448.7 ms | 516.1 ms | yes |
| `UPDATE` one row, autocommit | 6.4 ms | 4.4 ms | 4.3 ms | — |
| `INSERT` one row, autocommit | 4.9 ms | 4.3 ms | 4.4 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 114.87 µs | 84.98 µs | 88.98 µs | — |
| Check: totals after the writes | 51.8 ms | 50.9 ms | 57.5 ms | yes |
| Point lookups, 4 threads (per lookup) | 4.09 µs | 1.34 µs | 955 ns | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 327 MiB | 95 MiB | 104 MiB |
| after reads | 327 MiB | 98 MiB | 116 MiB |
| after writes | 296 MiB | 98 MiB | 51 MiB |
| at end | 297 MiB | 67 MiB | 57 MiB |
| peak | 404 MiB | 140 MiB | 178 MiB |

## Profile `large`: 1 GiB page cache each; squeal-db also 512 MiB per query and 512 MiB of scratch, SQLite and Turso temp tables in memory

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 1024 MiB, query memory 512 MiB, scratch 512 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 2.24 µs | 488 ns | 972 ns | — |
| Create 3 indexes (per index) | 709.1 ms | 182.3 ms | 428.5 ms | — |
| `ANALYZE` | 235.8 ms | 63.0 ms | 99.4 ms | — |
| Point lookup by primary key | 5.43 µs | 1.20 µs | 1.82 µs | yes |
| Secondary-index lookup + aggregate | 18.43 µs | 7.70 µs | 6.14 µs | yes |
| Index range (1 week of 52) + aggregate | 19.8 ms | 4.8 ms | 5.9 ms | yes |
| Full scan with `GROUP BY` | 150.1 ms | 226.2 ms | 166.2 ms | yes |
| Join + `GROUP BY` | 384.9 ms | 508.3 ms | 554.3 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 6.8 ms | 1.3 ms | 1.9 ms | yes |
| Sort the whole table (`ORDER BY` without an index) | 549.0 ms | 355.3 ms | 311.1 ms | yes |
| `IN (subquery)` | 125.2 ms | 67.9 ms | 84.3 ms | yes |
| Correlated `EXISTS` | 133.6 ms | 89.9 ms | 110.1 ms | yes |
| Correlated `NOT EXISTS` | 73.4 ms | 216.3 ms | 243.5 ms | yes |
| `UPDATE` one row, autocommit | 5.7 ms | 4.1 ms | 4.2 ms | — |
| `INSERT` one row, autocommit | 5.1 ms | 4.3 ms | 4.3 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 83.98 µs | 86.99 µs | 89.88 µs | — |
| Check: totals after the writes | 40.3 ms | 47.3 ms | 58.0 ms | yes |
| Point lookups, 4 threads (per lookup) | 2.61 µs | 1.86 µs | 916 ns | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 482 MiB | 192 MiB | 145 MiB |
| after reads | 749 MiB | 110 MiB | 337 MiB |
| after writes | 774 MiB | 110 MiB | 337 MiB |
| at end | 775 MiB | 221 MiB | 398 MiB |
| peak | 812 MiB | 222 MiB | 398 MiB |

