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
