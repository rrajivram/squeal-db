# squeal-db, SQLite and Turso: one SQL workload

size 200000, 4 reader threads, median of 3 run(s), each engine in its own process. Times are per operation; memory is the process's physical footprint.

## Profile `default`: each engine as it ships — squeal-db: 128 MiB page cache, 64 MiB per query; SQLite: ~2 MiB cache; Turso: 2,000-page cache

- **squeal**: squeal-db (squeal-sql on store), sent SQL text. Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **squealp**: squeal-db with prepared statements. Running with page cache 128 MiB, query memory 64 MiB, scratch 64 MiB
- **sqlite**: SQLite 3.53 through rusqlite, bundled. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache
- **turso**: Turso 0.8.2, the Rust rewrite of SQLite. Running with WAL, synchronous=FULL, fullfsync on, 2.0 MiB page cache

| phase | squeal | squealp | sqlite | turso | same answers |
|---|---:|---:|---:|---:|---|
| Bulk load, one transaction (per row) | 1.27 µs | 1.27 µs | 545 ns | 1.02 µs | — |
| Create 3 indexes (per index) | 61.5 ms | 55.3 ms | 35.2 ms | 83.6 ms | — |
| `ANALYZE` | 50.3 ms | 49.2 ms | 17.5 ms | 22.1 ms | — |
| Point lookup by primary key | 2.01 µs | 1.15 µs | 1.33 µs | 1.66 µs | yes |
| Secondary-index lookup + aggregate | 10.14 µs | 9.44 µs | 9.61 µs | 7.83 µs | yes |
| Index range (1 week of 52) + aggregate | 1.8 ms | 1.8 ms | 1.8 ms | 1.9 ms | yes |
| Full scan with `GROUP BY` | 30.2 ms | 30.4 ms | 45.1 ms | 39.8 ms | yes |
| Join + `GROUP BY` | 63.8 ms | 64.4 ms | 135.0 ms | 159.4 ms | yes |
| Join, filtered, `ORDER BY` + `LIMIT` | 888.45 µs | 901.76 µs | 423.89 µs | 447.82 µs | yes |
| Sort the whole table (`ORDER BY` without an index) | 78.1 ms | 78.4 ms | 63.6 ms | 64.0 ms | yes |
| `IN (subquery)` | 23.0 ms | 22.9 ms | 21.2 ms | 25.3 ms | yes |
| Correlated `EXISTS` | 24.4 ms | 24.7 ms | 26.5 ms | 24.9 ms | yes |
| Correlated `NOT EXISTS` | 13.0 ms | 13.0 ms | 66.7 ms | 42.5 ms | yes |
| `UPDATE` one row, autocommit | 4.8 ms | 4.9 ms | 4.1 ms | 4.1 ms | — |
| `INSERT` one row, autocommit | 4.6 ms | 4.6 ms | 4.1 ms | 4.3 ms | — |
| `UPDATE`, 100 rows per transaction (per row) | 87.01 µs | 76.82 µs | 75.01 µs | 83.08 µs | — |
| Check: totals after the writes | 11.5 ms | 9.0 ms | 13.8 ms | 19.8 ms | yes |
| Point lookups, 4 threads (per lookup) | 1.31 µs | 957 ns | 1.39 µs | 897 ns | yes |

| memory | squeal | squealp | sqlite | turso |
|---|---:|---:|---:|---:|
| after open | 3 MiB | 3 MiB | 3 MiB | 3 MiB |
| after load | 85 MiB | 85 MiB | 35 MiB | 48 MiB |
| after reads | 75 MiB | 77 MiB | 35 MiB | 62 MiB |
| after writes | 97 MiB | 99 MiB | 35 MiB | 63 MiB |
| at end | 97 MiB | 99 MiB | 45 MiB | 85 MiB |
| peak | 147 MiB | 144 MiB | 45 MiB | 86 MiB |

