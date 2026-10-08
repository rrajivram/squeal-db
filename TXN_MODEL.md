# Transaction model

One counter (the WAL's LSN clock) mints transaction ids and commit timestamps.
`TransactionManager` keeps one map, id → state: Active, Committing, Committed{ts},
Aborting, Aborted. Absent means committed long ago.

**Reads:** at `begin`, a transaction snapshots the ids not yet committed. It sees
its own writes, plus writers with smaller ids absent from that snapshot. A writer
mid-commit at `begin` waits for the outcome. No locks per row.

**Writes:** first committer wins: overwriting a row whose writer isn't
committed-before-you is a conflict.

**Aborts:** revert from version records.

**Cleanup:** versions older than the oldest active transaction are discarded.
