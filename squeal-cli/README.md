# squeal-cli

A terminal SQL REPL for squeal-db, built on
[`squeal-sql`](../squeal-sql/README.md).

```bash
cargo run -p squeal-cli --release -- path/to/my.db
```

At start-up it asks whether to use **(f)ile** or **(m)emory** storage:

- **File** opens the database at the path, or creates it.
- **Memory** keeps everything in a `MemFile` that is gone when you exit.

Pass a path. Without one, it falls back to a path inside the author's
checkout.

```
sql>> create table t (id integer not null, name varchar(20), primary key(id));
sql>> insert into t values (1, 'ada'), (2, 'grace');
sql>> select * from t where id > 1;
sql>> explain select * from t where id = 2;
```

- **Statements:** each line is run as typed, and can hold several
  statements separated by `;`.
- **History** is kept across sessions, in `history.txt` in the current
  directory.
- **Results** print as tables, with the row count and time.

| command | |
|---|---|
| `!help` | the SQL cheat sheet and these commands |
| `!print stats` | per-operator rows and timing for the last query |
| `!show table stats` | the optimizer's statistics for the current schema |
| `!reset stats` | zero the allocator counters |
| `exit` / Ctrl-D | quit |

The CLI enables `store`'s `alloc-tracking` feature, so it can print
allocation statistics: total allocated, peak, current usage, and a
size histogram. This makes it a convenient place to look at a query's
memory behaviour. Other users of `store` don't pay for the counting.

Built by Claude; it needed no architectural changes.
