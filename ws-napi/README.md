# ws-napi

A napi-rs-facing shim exposing squeal-db to Node.js — like `squeal-wasm`
(its browser counterpart), but backed by a real, **persistent**
`std::fs::File` instead of an ephemeral `MemFile`. Built for an embedded
database inside a Node.js host app (the motivating case: a custom n8n
node needing its own local, persistent store) — not for the browser.

Exposes one JS-facing type, `SquealDb`, with `execute(sql) -> JSON
string` (same shape as squeal-wasm's, plus the same `!help`/`!print
stats`/`!reset stats`/`!show table stats` REPL commands — see
`squeal_sql::help` for the shared SQL cheat sheet all three front-ends
render) and an explicit `close()`.

## Why napi-rs, not wasm-bindgen

`squeal-wasm` uses `wasm-bindgen`, built entirely around the browser
model (`WebAssembly.instantiate` + generated JS glue). WASI modules are
invoked completely differently (Node's own WASI runtime), and plain WASI
Preview 1's ABI is raw `i32`/`i64` only — wasm-bindgen doesn't target it
at all. napi-rs does: one `#[napi]`-annotated Rust API compiles two ways:

- A normal **native addon** (`.node`) — best performance, the default
  napi-rs story.
- **`wasm32-wasip1-threads`**, loaded by napi-rs's own generated Node.js
  WASI loader — no prebuilt binary needed per OS/arch (real filesystem
  access via Node's WASI preopening the host root), at some performance
  cost. This is the path an npm-distributed community node (e.g. for
  n8n) actually wants, to avoid shipping prebuilt binaries per
  OS/arch — napi-rs's own docs are explicit a WASI addon should be
  treated as trusted code, not a security sandbox.

`index.js` (generated) tries the native addon first and falls back to
WASI automatically — both are built from the exact same `src/lib.rs`.

## What real persistence needed, upstream in `store`

Two things confirmed broken **at runtime** under WASI despite compiling
cleanly — found by actually running a build under Node, not from reading
documentation (both were documented as supported):

- `std::fs::File::try_lock()` — every call returns "lock acquisition
  failed due to I/O error". WASI Preview 1 has no flock-equivalent
  syscall.
- `std::fs::File::try_clone()` — every call returns an `Unsupported`
  io::Error. WASI Preview 1 has no fd-duplication syscall either, and
  `Opener::do_clone` (used pervasively — every subsystem gets its own
  file handle) depends on it.

`store::memfile::WasiFile` (`target_os = "wasi"` only) is the fix: a
wrapper that remembers its own path, so "clone" and positioned I/O
(`pread`/`pwrite` — real positioned I/O syscalls exist on WASI, but
std's wrapper for them is still nightly-only) both work by reopening
that path or seeking a shared, mutex-guarded fd, instead of needing
`try_clone`/`try_lock` at all. See its own doc comment in
`store/src/memfile.rs` for the full story. Native code (including
`squeal-wasm`'s browser build) is unaffected — `WasiFile` is a new,
separate type, not a change to `std::fs::File`'s own native `Opener`
impl.

Verified beyond "compiles": built for `wasm32-wasip1-threads`, ran real
SQL through it in Node, then — the actual point of this crate — reopened
the **same file in a fresh Node process** three times in a row and
confirmed every row survived each restart.

## Build

```bash
cd ws-napi
npm install
npm run build            # native addon for this machine's platform
npx napi build --platform --release --target wasm32-wasip1-threads
```

Requires the `wasm32-wasip1-threads` Rust target:

```bash
rustup target add wasm32-wasip1-threads
```

## Try it

```bash
node --input-type=module -e "
import { SquealDb } from './index.js';
const db = new SquealDb('./my-app.db');   // creates it if it doesn't exist
console.log(db.execute('create table t (id integer not null, primary key(id))'));
console.log(db.execute('insert into t values (1)'));
console.log(db.execute('select * from t'));
db.close();
"
```

Run it again — the table and row are still there.

## Loading a CSV

Both forms work here (Node has a real filesystem, WASI included):

```js
db.execute('create table orders as copy from @/path/to/orders.csv');
db.createTableFromCsv('orders', csvText);   // CSV already in memory
```

Either one infers column names/types from the first rows (see
`squeal-sql/src/csv_infer.rs`), creates the table, and loads every row.

## Native tests

```bash
cargo test -p ws-napi
```
