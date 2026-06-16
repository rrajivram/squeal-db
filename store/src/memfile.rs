use std::{
    fs::OpenOptions,
    path::Path,
    sync::{Arc, RwLock},
};

use crate::db::{Meta, Opener};

#[derive(Debug, Default, Clone)]
pub(crate) struct MemFile {
    data: Arc<RwLock<Vec<u8>>>,
    seek_pos: usize,
}

impl MemFile {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}

impl Opener for MemFile {
    type Item = MemFile;
    fn open<P: AsRef<Path>>(_op: OpenOptions, _p: P) -> std::io::Result<MemFile> {
        Ok(MemFile::new())
    }

    fn do_sync(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn do_clone(&self) -> std::io::Result<Self::Item> {
        Ok(self.clone())
    }

    fn get_metadata(&self) -> std::io::Result<Meta> {
        Ok(Meta {
            len: self.data.read().unwrap().len() as u64,
        })
    }

    fn do_lock(&self) -> Result<(), std::fs::TryLockError> {
        Ok(())
    }
}

impl Opener for std::fs::File {
    type Item = std::fs::File;
    fn open<P: AsRef<Path>>(op: OpenOptions, p: P) -> std::io::Result<std::fs::File> {
        Ok(op.open(p)?)
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
}

impl std::io::Write for MemFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut data = self.data.write().unwrap();
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

impl std::io::Read for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut len = buf.len();
        let data = self.data.read().unwrap();
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
            std::io::SeekFrom::Current(c) => self.seek_pos += c as usize,
            std::io::SeekFrom::End(e) => {
                self.seek_pos = self.data.read().unwrap().len() - e as usize
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
}
