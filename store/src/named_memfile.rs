use std::{
    collections::HashMap,
    fs::OpenOptions,
    path::Path,
    sync::{Arc, LazyLock, Mutex},
};

use parking_lot::RwLock;

use crate::db::{Meta, Opener};

// A process-wide, name-keyed registry of buffers — what makes
// NamedMemFile able to represent "close, then reopen the same name" the
// way a real File can, unlike MemFile (whose `open` always hands back a
// fresh, empty buffer regardless of name). Kept separate from MemFile
// itself, and purely additive, so nothing depending on MemFile's
// existing "always fresh" behavior is affected.
type Registry = Mutex<HashMap<String, Arc<RwLock<Vec<u8>>>>>;
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// An in-memory `DBFile` backend, like `MemFile`, but `open()` returns the
/// same shared buffer a prior call for the same name last used instead of
/// always starting fresh — enough to let tests exercise genuine
/// close-then-reopen persistence (`Db::open` reading back what a prior
/// `Db::create`/session wrote) without touching real disk.
///
/// `OpenOptions`'s create-vs-open-only distinction can't be inspected on
/// stable Rust (its fields are private, with no portable getter), so
/// unlike a real `File`, opening a name that was never created doesn't
/// fail with `NotFound` — it fails downstream instead, the first time the
/// caller tries to read a real header out of the resulting empty buffer
/// (e.g. `Db::open`'s own `read_exact` on the file header). The
/// observable outcome callers care about — opening a database that was
/// never created is an error — still holds; only the specific IO error
/// kind differs from `File`'s.
#[derive(Debug, Clone)]
pub struct NamedMemFile {
    data: Arc<RwLock<Vec<u8>>>,
    seek_pos: usize,
}

impl NamedMemFile {
    /// Drops `name`'s entry (and its `.undo`/`.redo` siblings, mirroring
    /// `Db::create_core_db`'s own file layout) from the registry, if
    /// present. Call this directly rather than through
    /// `Db::<NamedMemFile>::delete` — that method is hardcoded to the real
    /// filesystem regardless of `F`, so it can't target this registry.
    pub fn delete(name: &str) {
        let mut reg = REGISTRY.lock().unwrap();
        reg.remove(name);
        reg.remove(&format!("{name}.undo"));
        reg.remove(&format!("{name}.redo"));
    }
}

impl Opener for NamedMemFile {
    type Item = NamedMemFile;

    fn open<P: AsRef<Path>>(_op: OpenOptions, p: P) -> std::io::Result<NamedMemFile> {
        let name = p.as_ref().to_string_lossy().into_owned();
        let mut reg = REGISTRY.lock().unwrap();
        let data = reg.entry(name).or_default().clone();
        Ok(NamedMemFile { data, seek_pos: 0 })
    }

    fn do_sync(&mut self) -> std::io::Result<()> {
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
        Ok(buf.len())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn truncate(&mut self) -> std::io::Result<()> {
        self.data.write().clear();
        Ok(())
    }
}

impl std::io::Write for NamedMemFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut data = self.data.write();
        let c_size = data.len();
        if self.seek_pos + buf.len() > c_size {
            data.resize(self.seek_pos + buf.len(), 0);
        }
        data[self.seek_pos..self.seek_pos + buf.len()].copy_from_slice(buf);
        self.seek_pos += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Read for NamedMemFile {
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

impl std::io::Seek for NamedMemFile {
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
mod tests;
