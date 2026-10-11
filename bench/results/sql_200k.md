# squeal-db, SQLite and Turso: one SQL workload

size 200000, 4 reader threads, median of 3 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 8.38 µs | 554 ns | 1.11 µs | — |
| Create 3 indexes (per index) | 715.2 ms | 36.9 ms | 98.5 ms | — |
| `ANALYZE` | 58.3 ms | 17.9 ms | 22.9 ms | — |
| Point lookup by primary key | 9.52 µs | 1.52 µs | 2.04 µs | yes |
| Secondary-index lookup + aggregate | 29.85 µs | 11.24 µs | 9.85 µs | yes |
| Index range (1 week of 52) + aggregate | 5.2 ms | 2.2 ms | 2.4 ms | yes |
| Full scan with `GROUP BY` | 46.3 ms | 50.0 ms | 45.9 ms | yes |
| Join + `GROUP BY` | 107.2 ms | 156.8 ms | 193.6 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.8 ms | 484.31 µs | 539.14 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 114.2 ms | 71.5 ms | 69.6 ms | yes |
| `IN (subquery)` | 35.0 ms | 24.4 ms | 32.0 ms | yes |
| Correlated `EXISTS` | 33.6 ms | 29.5 ms | 29.8 ms | yes |
| Correlated `NOT EXISTS` | 17.3 ms | 79.6 ms | 57.6 ms | yes |
| `UPDATE` one row, autocommit | 4.6 ms | 4.1 ms | 4.1 ms | — |
| `INSERT` one row, autocommit | 4.3 ms | 4.1 ms | 4.1 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 77.93 µs | 72.12 µs | 81.92 µs | — |
| Check: totals after the writes | 27.4 ms | 13.1 ms | 16.1 ms | yes |
| Point lookups, 4 threads (per lookup) | 5.75 µs | 1.80 µs | 1.09 µs | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 163 MiB | 35 MiB | 48 MiB |
| after reads | 158 MiB | 37 MiB | 65 MiB |
| after writes | 160 MiB | 37 MiB | 65 MiB |
| at end | 161 MiB | 47 MiB | 88 MiB |
| peak | 223 MiB | 47 MiB | 88 MiB |

## Profile `large`: 1 GiB page cache each; squeal-db also 512 MiB per query and 512 MiB of scratch, SQLite and Turso temp tables in memory

- **squeal**: squeal-db (squeal-sql on store). Running with page cache 1024 MiB, query memory 512 MiB, scratch 512 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 1024.0 MiB page cache

| phase | squeal | sqlite | turso | same answers |
|---|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 5.05 µs | 563 ns | 1.18 µs | — |
| Create 3 indexes (per index) | 618.5 ms | 45.0 ms | 95.1 ms | — |
| `ANALYZE` | 55.6 ms | 16.9 ms | 24.2 ms | — |
| Point lookup by primary key | 9.73 µs | 1.26 µs | 1.66 µs | yes |
| Secondary-index lookup + aggregate | 27.89 µs | 7.20 µs | 5.80 µs | yes |
| Index range (1 week of 52) + aggregate | 5.2 ms | 1.0 ms | 1.2 ms | yes |
| Full scan with `GROUP BY` | 46.8 ms | 51.3 ms | 38.9 ms | yes |
| Join + `GROUP BY` | 106.2 ms | 99.2 ms | 119.3 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.7 ms | 262.64 µs | 371.30 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 111.3 ms | 70.7 ms | 66.5 ms | yes |
| `IN (subquery)` | 35.2 ms | 12.2 ms | 16.2 ms | yes |
| Correlated `EXISTS` | 33.9 ms | 17.1 ms | 22.4 ms | yes |
| Correlated `NOT EXISTS` | 17.0 ms | 39.0 ms | 47.8 ms | yes |
| `UPDATE` one row, autocommit | 4.7 ms | 4.1 ms | 4.1 ms | — |
| `INSERT` one row, autocommit | 4.1 ms | 4.1 ms | 4.1 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 79.98 µs | 70.93 µs | 68.88 µs | — |
| Check: totals after the writes | 29.8 ms | 13.9 ms | 17.3 ms | yes |
| Point lookups, 4 threads (per lookup) | 5.77 µs | 1.75 µs | 992 ns | yes |

| memory | squeal | sqlite | turso |
|---|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB |
| after load | 163 MiB | 22 MiB | 49 MiB |
| after reads | 158 MiB | 32 MiB | 69 MiB |
| after writes | 160 MiB | 32 MiB | 69 MiB |
| at end | 161 MiB | 62 MiB | 88 MiB |
| peak | 224 MiB | 62 MiB | 88 MiB |

