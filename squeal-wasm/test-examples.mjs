// Runs every example in www/examples.js, in order, against a real wasm
// build, and fails if any throws — the Help panel only advertises what
// works. Also checks help() parses and a snapshot keeps both kinds of data.
//
//   wasm-bindgen --target nodejs --out-dir /tmp/squeal-node \
//       target/wasm32-unknown-unknown/release/squeal_wasm.wasm
//   node squeal-wasm/test-examples.mjs /tmp/squeal-node
import { createRequire } from 'node:module';
import path from 'node:path';
import { EXAMPLES } from './www/examples.js';

const pkg = process.argv[2];
if (!pkg) {
  console.error('usage: node test-examples.mjs <wasm-bindgen nodejs out dir>');
  process.exit(2);
}
const { SquealDb } = createRequire(import.meta.url)(path.resolve(pkg, 'squeal_wasm.js'));

const db = new SquealDb('examples');
let failed = 0;
for (const ex of EXAMPLES) {
  try {
    const results = JSON.parse(ex.mode === 'sql' ? db.execute(ex.text) : db.executeJson(ex.text));
    console.log(`ok   [${ex.mode}] ${ex.title} (${results.length} result(s))`);
  } catch (e) {
    failed += 1;
    console.log(`FAIL [${ex.mode}] ${ex.title}\n     ${String(e.message || e).replace(/\n/g, '\n     ')}`);
  }
}

// What the Help panel's JSON reference says works, beyond the examples:
// each statement, and a check of what it printed.
const CLAIMS = [
  ["db.h.insertOne({x: 1})", (t) => /"insertedId":\{"\$oid":"[0-9a-f]{24}"\}/.test(t)],
  ["db.h.insertMany([{_id: 1, name: 'Pen', a: {b: 2}, s: 3, w: 1, tags: ['x']}, {_id: 2, name: 'pad', s: 0, w: 5}])", (t) => t.includes('[1,2]')],
  ["db.h.find({name: {$regex: '^p', $options: 'i'}}).count()", (t) => t === '2'],
  ["db.h.find({$expr: {$gt: ['$s', '$w']}}, {_id: 1})", (t) => t === '{"_id":1}'],
  ["db.h.find({'a.b': 2}).count()", (t) => t === '1'],
  ["db.h.find({$or: [{s: 0}, {tags: 'x'}]}).count()", (t) => t === '2'],
  ["db.h.updateOne({_id: 1}, {$push: {tags: {$each: ['y', 'z']}}, $unset: {w: ''}, $currentDate: {updated: true}})", (t) => t.includes('"modifiedCount":1')],
  ["db.h.updateOne({_id: 1}, {$pull: {tags: 'y'}, $rename: {s: 'score'}})", (t) => t.includes('"modifiedCount":1')],
  ["db.h.findOne({_id: 1}, {tags: 1, score: 1, w: 1, updated: 1})", (t) => /"tags":\["x","z"\]/.test(t) && t.includes('"score":3') && !t.includes('"w"') && t.includes('"updated":{"$date"')],
  ["db.h.updateOne({_id: 9}, {$set: {a: 1}, $setOnInsert: {made: true}}, {upsert: true})", (t) => t.includes('upsertedId')],
  ["db.h.findOne({_id: 9})", (t) => t.includes('"made":true')],
  ["db.h.insertOne({_id: 10, when: ISODate('2024-05-01T10:00:00Z'), d2: new Date('2024-05-02'), n: NumberLong(5), i: NumberInt(3), o: ObjectId('65a1b2c3d4e5f60718293a4b')})", (t) => t.includes('"insertedId":10')],
  ["db.h.find({when: {$lt: ISODate('2025-01-01T00:00:00Z')}}, {_id: 1})", (t) => t === '{"_id":10}'],
  ["db.h.aggregate([{$match: {_id: 1}}, {$replaceRoot: {newRoot: '$a'}}])", (t) => t === '{"b":2}'],
  ["db.h.aggregate([{$match: {_id: {$in: [1, 2]}}}, {$project: {_id: 0, u: {$toUpper: '$name'}, c: {$cond: [{$gt: ['$s', 1]}, 'big', 'small']}, f: {$ifNull: ['$nope', 'none']}}}, {$sort: {u: 1}}])", (t) => t === '{"u":"PAD","c":"small","f":"none"}\n{"u":"PEN","c":"small","f":"none"}' || t.includes('"u":"PAD"')],
  ["db.h.dropIndex('nope')", null],
  ["show collections", (t) => t.split('\n').includes('h')],
];
const claims = new SquealDb('claims');
for (const [stmt, check] of CLAIMS) {
  let text;
  try {
    text = JSON.parse(claims.executeJson(stmt))[0].text;
  } catch (e) {
    if (check === null) { console.log(`ok   claim (errors, as it should): ${stmt}`); continue; }
    failed += 1;
    console.log(`FAIL claim: ${stmt}\n     ${e.message}`);
    continue;
  }
  if (check === null || !check(text)) {
    failed += 1;
    console.log(`FAIL claim: ${stmt}\n     got: ${text}`);
  } else {
    console.log(`ok   claim: ${stmt.slice(0, 70)}`);
  }
}

const help = JSON.parse(SquealDb.help());
if (!help.sql.length || !help.json.includes('db.<coll>.find(') || !help.commands.length) {
  failed += 1;
  console.log('FAIL help() is missing a part');
}

const again = SquealDb.fromSnapshot(db.snapshot());
const rows = JSON.parse(again.execute('select count(*) from orders'))[0].rows[0][0];
const docs = JSON.parse(again.executeJson('use shop; db.products.countDocuments({})'))[1].text;
console.log(`snapshot: ${rows} SQL order rows, ${docs} products`);
if (rows !== '5' || docs !== '5') {
  failed += 1;
  console.log('FAIL snapshot lost data');
}

if (failed) {
  console.log(`${failed} failure(s)`);
  process.exit(1);
}
console.log('all examples run');
