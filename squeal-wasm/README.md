# squeal-wasm

The wasm-facing shim for squeal-db — `squeal-cli`'s equivalent for a browser.
Exposes a single JS-facing type, `SquealDb`, with one method (`execute(sql)
-> JSON string`) covering every statement kind (DDL, DML, batched statements,
streaming `SELECT`s). See `src/lib.rs` for the full API and its doc comment
for why only `MemFile` is wired up.

## Try it in a browser

```bash
# from the repo root
cargo build --target wasm32-unknown-unknown -p squeal-wasm --release
wasm-bindgen --target web --out-dir squeal-wasm/www/pkg \
    target/wasm32-unknown-unknown/release/squeal_wasm.wasm

cd squeal-wasm/www
python3 -m http.server 8765
# open http://localhost:8765/ in a browser
```

`pkg/` is generated output (gitignored) — rerun the two build commands above
after any change to `squeal-wasm` or its dependencies. A real HTTP server is
required (not `file://`): browsers block `fetch()` of the `.wasm` file and
ES module imports over `file://`.

Requires `wasm-bindgen-cli`, matching the `wasm-bindgen` version in
`squeal-wasm/Cargo.toml`:

```bash
cargo install wasm-bindgen-cli --version 0.2.126 --locked
```

## JSON documents

The same database also holds MongoDB-style collections of JSON documents,
used with mongosh syntax through sq-json's shell:

```js
db.executeJson(`use shop
db.products.insertMany([{_id: 1, name: 'pen', price: 1.5}, {_id: 2, name: 'ink', price: 7.25}])
db.products.find({price: {$lt: 5}}).sort({price: -1})`);
db.jsonPrompt();           // "shop> " — the current database, "(txn)" in a transaction
SquealDb.help();           // JSON: the SQL reference, the !commands, the JSON method list
```

Statements are separated by `;` or line breaks (a line starting with `.`
continues the one before it). Each returns one `Message` whose text is what
the shell printed — documents as JSON, one per line. Collections are stored
in the same store as the SQL tables (as `sqjson.*` store tables, so a SQL
schema named `sqjson` would collide), and `snapshot()` saves both. The demo
page has a SQL / JSON documents switch, a Help panel generated from
`SquealDb.help()`, and runnable examples (`www/examples.js`).

## Loading a CSV

`CREATE TABLE t AS COPY FROM @path` infers column names/types from a CSV's
first rows, creates the table and loads every row — but `@path` needs a
filesystem, which a browser tab doesn't have (in this build that statement
fails with "operation not supported on this platform"). The browser
equivalent is a method that takes the CSV text itself:

```js
const text = await file.text();          // e.g. from <input type="file">
db.createTableFromCsv('orders', text);   // same inference + load, no path
```

The demo page's **Load CSV…** button does exactly this; the file never
leaves the browser.

## Keeping the database across reloads

The database lives in memory; persistence is whole-database snapshots.
`db.snapshot()` returns the committed state (data file + WAL, never an open
transaction's writes) as a `Uint8Array`, and `SquealDb.fromSnapshot(bytes)`
reopens it. `www/persist.js` stores those in IndexedDB:

```js
import { openPersistent } from './persist.js';
const persist = await openPersistent('my-db');   // restores if saved before
persist.db.execute(sql);
persist.changed();          // after anything that may have written: saves ~500 ms later

// Leaving the page before that save happened:
window.addEventListener('squeal:unsynced', (e) => {
  e.detail.event.preventDefault();   // ask the user before leaving
});
```

It also saves when the tab is hidden and tries once more on unload; that last
try can lose the race with the page closing, which is what the event is for.
`persist.clear()` deletes the saved copy. The demo page wires all of this up
(**Forget saved database** calls `clear()`).

Limits, by design — this is a scratchpad, not a server database: every save
writes the whole database (a 20k-row table is ~3 MB and saves in well under a
second), the browser can evict IndexedDB data under storage pressure, and two
tabs on the same key each save their own copy (last save wins).

## Try it in Node instead

Same build, different `wasm-bindgen` target:

```bash
cargo build --target wasm32-unknown-unknown -p squeal-wasm --release
wasm-bindgen --target nodejs --out-dir /tmp/squeal_wasm_out \
    target/wasm32-unknown-unknown/release/squeal_wasm.wasm
node --input-type=module -e "
import { SquealDb } from '/tmp/squeal_wasm_out/squeal_wasm.js';
const db = new SquealDb('scratch');
console.log(db.execute(\"create table t (id integer not null, primary key(id))\"));
console.log(db.execute('insert into t values (1)'));
console.log(db.execute('select * from t'));
"
```

## Native tests

```bash
cargo test -p squeal-wasm
```

## Checking the demo's examples against a real build

Every example in `www/examples.js` (what the Help panel offers) and every
JSON operator the Help panel's reference claims, run against the wasm
build — fails if any doesn't work:

```bash
wasm-bindgen --target nodejs --out-dir /tmp/squeal-node \
    target/wasm32-unknown-unknown/release/squeal_wasm.wasm
node squeal-wasm/test-examples.mjs /tmp/squeal-node
```

## Deploying the demo

`www/` is a static site, served by Cloudflare as the `squeal-db-demo`
Worker (`www/wrangler.jsonc`; `www/.assetsignore` keeps config files out):

```bash
cargo build --target wasm32-unknown-unknown -p squeal-wasm --release
wasm-bindgen --target web --out-dir squeal-wasm/www/pkg \
    target/wasm32-unknown-unknown/release/squeal_wasm.wasm
cd squeal-wasm/www && wrangler deploy
```

The `.wasm` must stay under Cloudflare's 25 MiB per-asset limit (it is ~13 MB).
