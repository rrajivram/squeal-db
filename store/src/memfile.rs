use std::{collections::HashMap, fs::OpenOptions, path::Path, sync::Arc};

use parking_lot::{Mutex, RwLock};

use crate::db::{Meta, Opener};

/// Phase 6: the files a MemFile can see as siblings — its own in-memory
/// "directory", shared by every clone and every file opened through
/// `open_sibling`. Fresh per `new`/`open`/`from_bytes`, so two databases
/// (or two tests) that reuse a name never see each other's segments.
type Namespace = Arc<Mutex<HashMap<String, MemFile>>>;

#[derive(Debug, Default, Clone)]
pub struct MemFile {
    data: Arc<RwLock<Vec<u8>>>,
    // TXN_SIMPLIFICATION_PLAN.md phase 0: the "disk" as of the last
    // `do_sync` call on any clone of this file. `data` is what the OS page
    // cache would hold; `synced` is what survives a power cut.
    synced: Arc<RwLock<Vec<u8>>>,
    // The byte range of `data` written since the last sync (lo, hi), so a
    // sync copies only what changed: the WAL syncs once per commit batch
    // and grows to megabytes, so copying the whole buffer every time made
    // the "mem" backend pay O(file) per commit (measured: ~15% of stress
    // throughput). `None` means nothing to copy.
    dirty: Arc<RwLock<Option<(usize, usize)>>>,
    seek_pos: usize,
    namespace: Namespace,
}

fn mark_dirty(dirty: &RwLock<Option<(usize, usize)>>, lo: usize, hi: usize) {
    let mut d = dirty.write();
    *d = Some(match *d {
        Some((a, b)) => (a.min(lo), b.max(hi)),
        None => (lo, hi),
    });
}

impl MemFile {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// A fresh, unshared file whose contents are `bytes` — both the live
    /// buffer and the synced copy, as if it had just been written and
    /// fsynced.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            data: Arc::new(RwLock::new(bytes.clone())),
            synced: Arc::new(RwLock::new(bytes)),
            dirty: Arc::new(RwLock::new(None)),
            seek_pos: 0,
            namespace: Namespace::default(),
        }
    }

    /// Registers a sibling named `path` holding `bytes` (synced) in this
    /// file's namespace — how the crash harness rebuilds "the disk" for a
    /// reopen: one data file plus every WAL segment that survived the cut.
    pub fn add_sibling_from_bytes(&self, path: &str, bytes: Vec<u8>) -> MemFile {
        let f = MemFile {
            namespace: self.namespace.clone(),
            ..MemFile::from_bytes(bytes)
        };
        self.namespace.lock().insert(path.to_string(), f.clone());
        f
    }

    /// Every (path, synced bytes) sibling in this namespace, sorted by path.
    pub fn synced_siblings(&self, prefix: &str) -> Vec<(String, Vec<u8>)> {
        let ns = self.namespace.lock();
        let mut out: Vec<(String, Vec<u8>)> = ns
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, f)| (k.clone(), f.synced_data()))
            .collect();
        out.sort();
        out
    }

    pub fn data(&self) -> Vec<u8> {
        self.data.read().clone()
    }

    /// The bytes that would survive a power cut right now: exactly the
    /// contents as of the most recent `do_sync` on any clone. Nothing
    /// written since — by `write`, `pwrite`, or `truncate` — is included.
    pub fn synced_data(&self) -> Vec<u8> {
        self.synced.read().clone()
    }

    /// A new, independent file holding only `synced_data()` — the crash
    /// harness's "what the disk has after the power cut" file.
    pub fn synced_snapshot(&self) -> MemFile {
        MemFile::from_bytes(self.synced_data())
    }
}

impl Opener for MemFile {
    type Item = MemFile;
    /// A fresh file in a fresh namespace, registered there under `p` so its
    /// siblings (WAL segments) can find each other.
    fn open<P: AsRef<Path>>(_op: OpenOptions, p: P) -> std::io::Result<MemFile> {
        let f = MemFile::new();
        f.namespace
            .lock()
            .insert(p.as_ref().to_string_lossy().into_owned(), f.clone());
        Ok(f)
    }

    fn open_sibling(&self, path: &str, _op: OpenOptions) -> std::io::Result<MemFile> {
        let mut ns = self.namespace.lock();
        if let Some(f) = ns.get(path) {
            let mut f = f.clone();
            f.seek_pos = 0;
            return Ok(f);
        }
        let f = MemFile {
            namespace: self.namespace.clone(),
            ..MemFile::new()
        };
        ns.insert(path.to_string(), f.clone());
        Ok(f)
    }

    fn list_siblings(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        Ok(self
            .namespace
            .lock()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn remove_sibling(&self, path: &str) -> std::io::Result<()> {
        self.namespace.lock().remove(path);
        Ok(())
    }

    fn do_sync(&mut self) -> std::io::Result<()> {
        let data = self.data.read();
        let range = self.dirty.write().take();
        let mut synced = self.synced.write();
        // Length first: a truncate shrinks, an append grows.
        synced.resize(data.len(), 0);
        if let Some((lo, hi)) = range {
            let hi = hi.min(data.len());
            if lo < hi {
                synced[lo..hi].copy_from_slice(&data[lo..hi]);
            }
        }
        Ok(())
    }

    fn do_clone(&self) -> std::io::Result<Self::Item> {
        Ok(self.clone())
    }

    fn get_metadata(&self) -> std::io::Result<Meta> {
        Ok(Meta {
            len: self.data.read().len() as u64,
        })
    }

    fn do_lock(&self) -> Result<(), std::fs::TryLockError> {
        Ok(())
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        let data = self.data.read();
        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0); // at or past EOF
        }
        let n = (data.len() - offset).min(buf.len());
        buf[..n].copy_from_slice(&data[offset..offset + n]);
        Ok(n)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let mut data = self.data.write();
        let offset = offset as usize;
        if offset + buf.len() > data.len() {
            data.resize(offset + buf.len(), 0);
        }
        data[offset..offset + buf.len()].copy_from_slice(buf);
        mark_dirty(&self.dirty, offset, offset + buf.len());
        Ok(buf.len())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn truncate(&mut self) -> std::io::Result<()> {
        self.data.write().clear();
        // Everything that gets written from here on is new content.
        *self.dirty.write() = None;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Opener for std::fs::File {
    type Item = std::fs::File;
    fn open<P: AsRef<Path>>(op: OpenOptions, p: P) -> std::io::Result<std::fs::File> {
        op.open(p)
    }

    fn open_sibling(&self, path: &str, op: OpenOptions) -> std::io::Result<std::fs::File> {
        op.open(path)
    }

    fn list_siblings(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        list_files_with_prefix(prefix)
    }

    fn remove_sibling(&self, path: &str) -> std::io::Result<()> {
        match std::fs::remove_file(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            r => r,
        }
    }

    fn do_sync(&mut self) -> std::io::Result<()> {
        self.sync_data()
    }

    fn do_clone(&self) -> std::io::Result<Self::Item> {
        self.try_clone()
    }

    fn get_metadata(&self) -> std::io::Result<Meta> {
        let m = self.metadata()?;
        Ok(Meta { len: m.len() })
    }

    fn do_lock(&self) -> Result<(), std::fs::TryLockError> {
        self.try_lock()
    }

    fn truncate(&mut self) -> std::io::Result<()> {
        self.set_len(0)?;
        Ok(())
    }

    #[cfg(unix)]
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buf, offset)
    }
    #[cfg(unix)]
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        std::os::unix::fs::FileExt::write_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(self, buf, offset)
    }
    #[cfg(windows)]
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        std::os::windows::fs::FileExt::seek_write(self, buf, offset)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Real-filesystem sibling listing: every entry in `prefix`'s directory
/// whose name starts with `prefix`'s file name, returned as full paths.
pub fn list_files_with_prefix(prefix: &str) -> std::io::Result<Vec<String>> {
    let p = Path::new(prefix);
    let dir = match p.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    let stem = p
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&stem) {
            out.push(if p.parent().map(|d| d.as_os_str().is_empty()).unwrap_or(true) {
                name
            } else {
                dir.join(name).to_string_lossy().into_owned()
            });
        }
    }
    Ok(out)
}

impl std::io::Write for MemFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut data = self.data.write();
        let c_size = data.len();
        if self.seek_pos + buf.len() > c_size {
            data.resize(self.seek_pos + buf.len(), 0);
        }
        data[self.seek_pos..self.seek_pos + buf.len()].copy_from_slice(buf);
        mark_dirty(&self.dirty, self.seek_pos, self.seek_pos + buf.len());
        self.seek_pos += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Read for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut len = buf.len();
        let data = self.data.read();
        if self.seek_pos >= data.len() {
            return Ok(0); // at or past EOF
        }
        if self.seek_pos + len > data.len() {
            len = data.len() - self.seek_pos;
        }
        buf[0..len].copy_from_slice(&data[self.seek_pos..self.seek_pos + len]);
        self.seek_pos += len;
        Ok(len)
    }
}

impl std::io::Seek for MemFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            std::io::SeekFrom::Current(c) => {
                self.seek_pos = (self.seek_pos as i64 + c).max(0) as usize;
            }
            std::io::SeekFrom::End(e) => {
                let len = self.data.read().len() as i64;
                self.seek_pos = (len + e).max(0) as usize;
            }
            std::io::SeekFrom::Start(s) => self.seek_pos = s as usize,
        }
        Ok(self.seek_pos as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    use crate::memfile::MemFile;

    #[test]
    fn test_mem_file() {
        let mut f = MemFile::new();
        assert!(f.write(b"abcdef").is_ok());
        assert!(f.seek(SeekFrom::Start(0)).is_ok());
        let mut buf = vec![0u8; 10];
        assert!(f.read(&mut buf).is_ok());
        assert_eq!(&buf[0..6], b"abcdef");
    }

    #[test]
    fn test_seek_end_zero() {
        let mut f = MemFile::new();
        f.write_all(b"hello world").unwrap();
        let pos = f.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(pos, 11);
        let mut buf = vec![0u8; 4];
        let n = f.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_seek_end_negative() {
        let mut f = MemFile::new();
        f.write_all(b"hello world").unwrap();
        let pos = f.seek(SeekFrom::End(-5)).unwrap();
        assert_eq!(pos, 6);
        let mut buf = vec![0u8; 5];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_seek_current_forward() {
        let mut f = MemFile::new();
        f.write_all(b"hello world").unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.seek(SeekFrom::Current(6)).unwrap();
        let mut buf = vec![0u8; 5];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_seek_current_backward() {
        let mut f = MemFile::new();
        f.write_all(b"hello world").unwrap();
        f.seek(SeekFrom::Start(9)).unwrap();
        f.seek(SeekFrom::Current(-3)).unwrap();
        let mut buf = vec![0u8; 5];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_overwrite_at_offset() {
        let mut f = MemFile::new();
        f.write_all(b"hello world").unwrap();
        f.seek(SeekFrom::Start(6)).unwrap();
        f.write_all(b"Rust!").unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0u8; 11];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello Rust!");
    }

    #[test]
    fn test_clone_shares_data() {
        let mut f = MemFile::new();
        f.write_all(b"shared").unwrap();
        let mut g = f.clone();
        g.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0u8; 6];
        g.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"shared");
        // Write via f is visible through g
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(b"SHARED").unwrap();
        g.seek(SeekFrom::Start(0)).unwrap();
        g.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"SHARED");
    }

    #[test]
    fn test_synced_snapshot_holds_only_what_was_synced() {
        use crate::db::Opener;
        let mut f = MemFile::new();
        f.write_all(b"durable").unwrap();
        f.do_sync().unwrap();
        f.write_all(b" and not").unwrap();
        // pwrite through a clone is also unsynced until someone syncs.
        f.clone().pwrite(b"D", 0).unwrap();
        assert_eq!(f.data(), b"Durable and not".to_vec());
        assert_eq!(f.synced_data(), b"durable".to_vec());
        let snap = f.synced_snapshot();
        assert_eq!(snap.data(), b"durable".to_vec());
        // A sync through any clone publishes the shared live buffer.
        f.clone().do_sync().unwrap();
        assert_eq!(f.synced_data(), b"Durable and not".to_vec());
        // The snapshot is independent of the original.
        assert_eq!(snap.data(), b"durable".to_vec());
    }

    #[test]
    fn test_synced_snapshot_tracks_truncate_and_partial_rewrites() {
        use crate::db::Opener;
        let mut f = MemFile::new();
        f.write_all(b"0123456789").unwrap();
        f.do_sync().unwrap();
        // Overwrite a middle range without syncing: invisible.
        f.pwrite(b"ab", 3).unwrap();
        assert_eq!(f.synced_data(), b"0123456789".to_vec());
        f.do_sync().unwrap();
        assert_eq!(f.synced_data(), b"012ab56789".to_vec());
        // Truncate then rewrite shorter content: the synced copy shrinks.
        f.truncate().unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(b"xy").unwrap();
        assert_eq!(f.synced_data(), b"012ab56789".to_vec());
        f.do_sync().unwrap();
        assert_eq!(f.synced_data(), b"xy".to_vec());
    }

    #[test]
    fn test_siblings_share_a_namespace_but_files_from_new_do_not() {
        use crate::db::Opener;
        let opts = std::fs::OpenOptions::new();
        let a = MemFile::open(opts.clone(), "db").unwrap();
        let mut seg1 = a.open_sibling("db.wal.1", opts.clone()).unwrap();
        seg1.write_all(b"one").unwrap();
        let _seg2 = a.open_sibling("db.wal.2", opts.clone()).unwrap();
        let mut listed = a.list_siblings("db.wal.").unwrap();
        listed.sort();
        assert_eq!(listed, vec!["db.wal.1".to_string(), "db.wal.2".to_string()]);
        // A clone, and a sibling opened from a sibling, see the same set.
        assert_eq!(a.clone().list_siblings("db.wal.").unwrap().len(), 2);
        assert_eq!(seg1.list_siblings("db.wal.").unwrap().len(), 2);
        // Reopening a sibling by name yields the same bytes, positioned at 0.
        let mut again = a.open_sibling("db.wal.1", opts.clone()).unwrap();
        let mut buf = vec![0u8; 3];
        again.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"one");
        // Another namespace with the same names is unrelated.
        let b = MemFile::open(opts.clone(), "db").unwrap();
        assert!(b.list_siblings("db.wal.").unwrap().is_empty());
        assert!(MemFile::new().list_siblings("").unwrap().is_empty());
        // Removal is per name and idempotent.
        a.remove_sibling("db.wal.1").unwrap();
        a.remove_sibling("db.wal.1").unwrap();
        assert_eq!(a.list_siblings("db.wal.").unwrap(), vec!["db.wal.2".to_string()]);
    }

    #[test]
    fn test_synced_siblings_capture_only_synced_bytes() {
        use crate::db::Opener;
        let opts = std::fs::OpenOptions::new();
        let a = MemFile::open(opts.clone(), "db").unwrap();
        let mut seg = a.open_sibling("db.wal.1", opts.clone()).unwrap();
        seg.write_all(b"durable").unwrap();
        seg.do_sync().unwrap();
        seg.write_all(b"-not").unwrap();
        let snap = a.synced_siblings("db.wal.");
        assert_eq!(snap, vec![("db.wal.1".to_string(), b"durable".to_vec())]);
        // Rebuilding a disk image from the snapshot: siblings are registered
        // in the new file's own namespace.
        let disk = MemFile::from_bytes(b"data".to_vec());
        for (path, bytes) in snap {
            disk.add_sibling_from_bytes(&path, bytes);
        }
        assert_eq!(disk.list_siblings("db.wal.").unwrap(), vec!["db.wal.1".to_string()]);
        assert!(a.list_siblings("other").unwrap().is_empty());
    }

    #[test]
    fn test_read_partial_data() {
        let mut f = MemFile::new();
        f.write_all(b"abcde").unwrap();
        f.seek(SeekFrom::Start(2)).unwrap();
        let mut buf = vec![0u8; 10]; // request more than available
        let n = f.read(&mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..3], b"cde");
    }
}
