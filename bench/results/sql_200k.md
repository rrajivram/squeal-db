# squeal-db, SQLite and Turso: one SQL workload

size 200000, 4 reader threads, median of 3 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store), sent SQL text. Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **squealp**: squeal-db with prepared statements. Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | squealp | sqlite | turso | same answers |
|---|---:|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 1.85 µs | 1.85 µs | 551 ns | 1.02 µs | — |
| Create 3 indexes (per index) | 121.6 ms | 125.8 ms | 36.1 ms | 93.2 ms | — |
| `ANALYZE` | 48.9 ms | 48.0 ms | 17.0 ms | 23.0 ms | — |
| Point lookup by primary key | 2.07 µs | 1.16 µs | 1.39 µs | 1.80 µs | yes |
| Secondary-index lookup + aggregate | 11.01 µs | 9.82 µs | 9.88 µs | 8.32 µs | yes |
| Index range (1 week of 52) + aggregate | 2.1 ms | 2.0 ms | 1.8 ms | 2.1 ms | yes |
| Full scan with `GROUP BY` | 30.8 ms | 30.5 ms | 46.7 ms | 41.4 ms | yes |
| Join + `GROUP BY` | 74.7 ms | 72.8 ms | 138.0 ms | 168.9 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 916.05 µs | 941.70 µs | 439.39 µs | 486.55 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 100.0 ms | 93.1 ms | 65.6 ms | 67.0 ms | yes |
| `IN (subquery)` | 23.3 ms | 23.5 ms | 21.6 ms | 27.5 ms | yes |
| Correlated `EXISTS` | 25.4 ms | 25.5 ms | 27.2 ms | 25.8 ms | yes |
| Correlated `NOT EXISTS` | 13.4 ms | 13.5 ms | 67.8 ms | 45.6 ms | yes |
| `UPDATE` one row, autocommit | 4.9 ms | 4.7 ms | 4.1 ms | 4.1 ms | — |
| `INSERT` one row, autocommit | 4.7 ms | 4.2 ms | 4.1 ms | 4.1 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 89.91 µs | 83.97 µs | 74.04 µs | 70.95 µs | — |
| Check: totals after the writes | 12.1 ms | 13.7 ms | 16.2 ms | 16.6 ms | yes |
| Point lookups, 4 threads (per lookup) | 1.38 µs | 1.00 µs | 1.49 µs | 971 ns | yes |

| memory | squeal | squealp | sqlite | turso |
|---|---:|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB | 3 MiB |
| after load | 72 MiB | 72 MiB | 35 MiB | 48 MiB |
| after reads | 75 MiB | 75 MiB | 35 MiB | 62 MiB |
| after writes | 108 MiB | 107 MiB | 35 MiB | 63 MiB |
| at end | 108 MiB | 108 MiB | 45 MiB | 86 MiB |
| peak | 143 MiB | 143 MiB | 46 MiB | 86 MiB |

