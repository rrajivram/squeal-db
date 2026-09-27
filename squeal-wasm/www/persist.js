// Keeps a squeal-wasm database across page loads by saving whole-database
// snapshots (SquealDb.snapshot()) to IndexedDB under `key`, and reopening
// from the saved one next time. Meant for small databases: every save
// writes the whole thing.
//
//   const persist = await openPersistent('my-db');
//   persist.db.execute(sql);
//   persist.changed();   // after anything that might have written
//
// Saves happen `delayMs` after the last changed() call, and right away when
// the tab is hidden. If the page is being unloaded with changes not yet
// saved, a `squeal:unsynced` event is dispatched on `window` (detail:
// { key, event }, where `event` is the beforeunload event — call
// `event.preventDefault()` on it to have the browser ask the user before
// leaving). Only one tab should have a given key open: two tabs each save
// their own copy, and the last save wins.
import { SquealDb } from './pkg/squeal_wasm.js';

const IDB_NAME = 'squeal-wasm';
const STORE = 'snapshots';

// Runs one request against the snapshots store and resolves with its
// result once the transaction has committed.
function idb(mode, request) {
  return new Promise((resolve, reject) => {
    const open = indexedDB.open(IDB_NAME, 1);
    open.onupgradeneeded = () => open.result.createObjectStore(STORE);
    open.onerror = () => reject(open.error);
    open.onsuccess = () => {
      const conn = open.result;
      const tx = conn.transaction(STORE, mode);
      const req = request(tx.objectStore(STORE));
      tx.oncomplete = () => { conn.close(); resolve(req.result); };
      tx.onerror = tx.onabort = () => { conn.close(); reject(tx.error); };
    };
  });
}

export async function openPersistent(key, { delayMs = 500, onSaved = () => {} } = {}) {
  const saved = await idb('readonly', (s) => s.get(key));
  const db = saved ? SquealDb.fromSnapshot(saved) : new SquealDb(key);
  let savedGen = db.syncGeneration();
  let timer = null;
  let cleared = false;

  const unsynced = () => !cleared && db.syncGeneration() !== savedGen;

  // IndexedDB runs readwrite transactions on the same store in the order
  // they were created, so overlapping saves land oldest-first and savedGen
  // only ever moves forward.
  async function sync() {
    clearTimeout(timer);
    timer = null;
    if (!unsynced()) return;
    const gen = db.syncGeneration(); // before snapshot(): anything later counts as unsaved
    await idb('readwrite', (s) => s.put(db.snapshot(), key));
    savedGen = Math.max(savedGen, gen);
    onSaved();
  }

  function changed() {
    if (!timer && unsynced()) {
      timer = setTimeout(() => sync().catch((e) => console.error('squeal: save failed', e)), delayMs);
    }
  }

  // Deletes the saved snapshot and stops saving; reload the page to start
  // over with an empty database.
  async function clear() {
    cleared = true;
    clearTimeout(timer);
    await idb('readwrite', (s) => s.delete(key));
  }

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      sync().catch((e) => console.error('squeal: save failed', e));
    }
  });
  window.addEventListener('beforeunload', (event) => {
    if (!unsynced()) return;
    // Best effort: the page may be gone before this save completes.
    sync().catch(() => {});
    window.dispatchEvent(new CustomEvent('squeal:unsynced', { detail: { key, event } }));
  });

  return { db, changed, sync, unsynced, clear };
}
