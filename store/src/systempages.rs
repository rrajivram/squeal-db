//! The store's three system "pages" — the table catalog (page 0), the
//! sequence/generator state (page 1) and the free-page list (page 2) — as
//! versioned, growable page CHAINS.
//!
//! Persistence versioning Stage 6. Each of those used to be exactly one
//! pinned page: a catalog with more tables than fit on it, or a free list
//! longer than one page, could not be persisted at all (STORE_AUDIT.md S6
//! papered over that by failing `create_table` cleanly). Now each is a chain:
//! the fixed head page (0/1/2) plus any number of continuation pages
//! allocated from the ordinary page allocator, linked through `next_page`.
//!
//! Layout of every chain page: one reserved HEADER tuple (id
//! `HEADER_TUPLE_ID`, which no payload id can equal) followed by payload
//! tuples with ids assigned sequentially across the whole chain. The header
//! is `write_versioned(SYSTEM_CHAIN_VERSION, [kind u8][position u32 LE])`.
//! A head page WITHOUT a header tuple is the pre-Stage-6 layout (one page, no
//! header, unversioned payloads) and is read as such — `ChainRead::legacy` —
//! so an existing database opens unchanged and is upgraded by its next
//! checkpoint. Each payload carries its OWN version tag too, so a payload
//! shape (say `Table`) can change without touching the chain layout.
//!
//! A chain never shrinks: pages it no longer needs stay chained as empty
//! header-only pages. Releasing them would mutate the very free list being
//! written; the cost is bounded by the chain's historical peak.

use std::collections::HashSet;

use postcard::{from_bytes, to_allocvec};

use crate::{
    buffer::PageBuffer,
    db::{DBFile, DBSizeType},
    error::StoreError,
    page::{Page, PageId},
    tuple::{DBIdType, Tuple},
    versioned::{read_version_tag, unsupported_version, write_versioned},
};

/// Version of the chain-page layout (header tuple + sequential payload ids).
pub(crate) const SYSTEM_CHAIN_VERSION: u16 = 1;

/// Version of each payload kind's own encoding. Independent of the chain
/// version and of each other: bump one only when that payload's shape
/// changes, after freezing the old shape (see `versioned.rs`).
pub(crate) const CATALOG_PAYLOAD_VERSION: u16 = 1;
pub(crate) const GENERATOR_PAYLOAD_VERSION: u16 = 1;
pub(crate) const FREE_LIST_PAYLOAD_VERSION: u16 = 1;

/// Reserved: payload ids count up from 0 and can never reach this.
pub(crate) const HEADER_TUPLE_ID: DBSizeType = DBSizeType::MAX;

/// Free-page ids per payload tuple. Small enough that one tuple (worst case
/// ~10 bytes per varint id) always fits comfortably on a minimum-size page.
pub(crate) const FREE_LIST_CHUNK: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum SystemKind {
    Catalog = 0,
    Generator = 1,
    FreeList = 2,
}

impl SystemKind {
    pub(crate) const ALL: [SystemKind; 3] =
        [SystemKind::Catalog, SystemKind::Generator, SystemKind::FreeList];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        match self {
            SystemKind::Catalog => "catalog",
            SystemKind::Generator => "generator",
            SystemKind::FreeList => "free-list",
        }
    }
}

fn header_tuple(kind: SystemKind, position: u32) -> Tuple {
    let body = write_versioned(SYSTEM_CHAIN_VERSION, |out| {
        out.push(kind as u8);
        out.extend_from_slice(&position.to_le_bytes());
    });
    Tuple::new(HEADER_TUPLE_ID, &body)
}

fn parse_header(
    tuple: &Tuple,
    kind: SystemKind,
    expected_position: u32,
    page: PageId,
) -> Result<(), StoreError> {
    let (version, rest) = read_version_tag(&tuple.data)?;
    match version {
        1 => {
            let [k, p0, p1, p2, p3] = rest else {
                return Err(StoreError::Corruption(format!(
                    "{} system page {page:?}: header is {} byte(s), expected 5",
                    kind.name(),
                    rest.len()
                )));
            };
            let position = u32::from_le_bytes([*p0, *p1, *p2, *p3]);
            if *k != kind as u8 || position != expected_position {
                return Err(StoreError::Corruption(format!(
                    "{} system page {page:?}: header says kind {k} position {position}, expected \
                     kind {} position {expected_position}",
                    kind.name(),
                    kind as u8
                )));
            }
            Ok(())
        }
        other => Err(unsupported_version("system page chain", other)),
    }
}

/// What `read_chain` found.
#[derive(Debug)]
pub(crate) struct ChainRead {
    /// The head page had no header tuple: the pre-Stage-6, single-page,
    /// unversioned layout. `payloads` are then the raw tuples' data and the
    /// caller decodes them with its legacy (unversioned) codec.
    pub(crate) legacy: bool,
    /// Continuation page ids, in chain order (empty for a legacy chain).
    pub(crate) continuations: Vec<PageId>,
    /// Payload bytes in order across the whole chain.
    pub(crate) payloads: Vec<Vec<u8>>,
}

pub(crate) fn read_chain<F: DBFile + 'static>(
    buffer: &PageBuffer<F>,
    head: PageId,
    kind: SystemKind,
) -> Result<ChainRead, StoreError> {
    let mut payloads = Vec::new();
    let mut continuations = Vec::new();
    let mut visited: HashSet<PageId> = HashSet::from([head]);
    let mut cur = head;
    let mut position = 0u32;
    loop {
        let page = buffer.get_page(cur)?;
        let mut header = None;
        let mut body = Vec::new();
        for t in page.iter() {
            if t.id == DBIdType::Int(HEADER_TUPLE_ID) {
                header = Some(t);
            } else {
                body.push(t);
            }
        }
        match header {
            None if position == 0 => {
                body.sort_by(|a, b| a.id.cmp(&b.id));
                return Ok(ChainRead {
                    legacy: true,
                    continuations,
                    payloads: body.into_iter().map(|t| t.data.to_vec()).collect(),
                });
            }
            None => {
                return Err(StoreError::Corruption(format!(
                    "{} system page {cur:?} (chain position {position}) has no header",
                    kind.name()
                )));
            }
            Some(h) => parse_header(&h, kind, position, cur)?,
        }
        body.sort_by(|a, b| a.id.cmp(&b.id));
        payloads.extend(body.into_iter().map(|t| t.data.to_vec()));
        let next = page.get_next_page();
        if !next.is_valid_next_page() {
            break;
        }
        if !visited.insert(next) {
            return Err(StoreError::Corruption(format!(
                "{} system chain revisits page {next:?}: the chain has a cycle",
                kind.name()
            )));
        }
        continuations.push(next);
        cur = next;
        position += 1;
    }
    Ok(ChainRead {
        legacy: false,
        continuations,
        payloads,
    })
}

/// Splits `payloads` across as many fresh pinned pages as needed, each
/// starting with its header tuple. A single payload that cannot fit on an
/// otherwise empty page is an error, never silently dropped.
fn partition<F: DBFile + 'static>(
    buffer: &PageBuffer<F>,
    kind: SystemKind,
    payloads: &[Vec<u8>],
) -> Result<Vec<Page>, StoreError> {
    let new_page = |position: u32| -> Result<Page, StoreError> {
        let page = Page::new_pinned(buffer.page_size(), buffer.page_overhead());
        page.add_tuple(header_tuple(kind, position))?;
        Ok(page)
    };
    let mut pages = vec![new_page(0)?];
    for (i, bytes) in payloads.iter().enumerate() {
        let tuple = Tuple::new(i as DBSizeType, bytes);
        if !pages.last().unwrap().can_store(&tuple) {
            let next = new_page(pages.len() as u32)?;
            if !next.can_store(&tuple) {
                return Err(StoreError::TupleTooLarge(
                    tuple.size() as DBSizeType,
                    buffer.page_size() as usize - buffer.page_overhead(),
                ));
            }
            pages.push(next);
        }
        pages.last().unwrap().add_tuple(tuple)?;
    }
    Ok(pages)
}

/// Writes `kind`'s chain: `head` plus `continuations` (grown from the page
/// allocator as needed, never shrunk). `make_payloads` is called again after
/// every allocation because, for the free list, allocating a continuation
/// page pops an id off the very list being serialized.
pub(crate) fn write_chain<F: DBFile + 'static>(
    buffer: &PageBuffer<F>,
    head: PageId,
    kind: SystemKind,
    mut make_payloads: impl FnMut() -> Result<Vec<Vec<u8>>, StoreError>,
    continuations: &mut Vec<PageId>,
) -> Result<(), StoreError> {
    let mut pages = loop {
        let payloads = make_payloads()?;
        let pages = partition(buffer, kind, &payloads)?;
        if pages.len() <= 1 + continuations.len() {
            break pages;
        }
        continuations.push(buffer.alloc_page(true)?);
    };
    // Never shrink: keep every allocated continuation page chained, empty.
    while pages.len() < 1 + continuations.len() {
        let page = Page::new_pinned(buffer.page_size(), buffer.page_overhead());
        page.add_tuple(header_tuple(kind, pages.len() as u32))?;
        pages.push(page);
    }
    let ids: Vec<PageId> = std::iter::once(head)
        .chain(continuations.iter().copied())
        .collect();
    for (i, page) in pages.iter().enumerate() {
        if let Some(next) = ids.get(i + 1) {
            page.set_next_page(*next)?;
        }
        buffer.write_page(ids[i], page)?;
    }
    Ok(())
}

fn encode_with_version<T: serde::Serialize>(version: u16, value: &T) -> Result<Vec<u8>, StoreError> {
    let body = to_allocvec(value)?;
    Ok(write_versioned(version, |out| out.extend_from_slice(&body)))
}

// ---------------------------------------------------------------------------
// Payload codecs. Each payload is `[u16 version][postcard body of that
// version's shape]`. Version 1's shapes are the live types today; when one
// changes, freeze a `...V1Shape` copy and branch here first.
// ---------------------------------------------------------------------------

pub(crate) fn encode_catalog_entry(table: &crate::table::Table) -> Result<Vec<u8>, StoreError> {
    encode_with_version(CATALOG_PAYLOAD_VERSION, table)
}

pub(crate) fn decode_catalog_entry(bytes: &[u8]) -> Result<crate::table::Table, StoreError> {
    let (version, body) = read_version_tag(bytes)?;
    match version {
        1 => Ok(from_bytes(body)?),
        other => Err(unsupported_version("catalog entry", other)),
    }
}

pub(crate) fn encode_generator_entry(name: &str, value: DBSizeType) -> Result<Vec<u8>, StoreError> {
    encode_with_version(GENERATOR_PAYLOAD_VERSION, &(name, value))
}

pub(crate) fn decode_generator_entry(bytes: &[u8]) -> Result<(String, DBSizeType), StoreError> {
    let (version, body) = read_version_tag(bytes)?;
    match version {
        1 => Ok(from_bytes(body)?),
        other => Err(unsupported_version("generator entry", other)),
    }
}

pub(crate) fn encode_free_list(free: &[PageId]) -> Result<Vec<Vec<u8>>, StoreError> {
    free.chunks(FREE_LIST_CHUNK)
        .map(|chunk| encode_with_version(FREE_LIST_PAYLOAD_VERSION, &chunk.to_vec()))
        .collect()
}

pub(crate) fn decode_free_list_chunk(bytes: &[u8]) -> Result<Vec<PageId>, StoreError> {
    let (version, body) = read_version_tag(bytes)?;
    match version {
        1 => Ok(from_bytes::<Vec<PageId>>(body)?),
        other => Err(unsupported_version("free-list chunk", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_codecs_round_trip() {
        let (n, v) = decode_generator_entry(&encode_generator_entry("seq", 42).unwrap()).unwrap();
        assert_eq!((n.as_str(), v), ("seq", 42));

        let free: Vec<PageId> = (5..5 + 300u64).map(PageId::from).collect();
        let chunks = encode_free_list(&free).unwrap();
        assert_eq!(chunks.len(), 3, "300 ids at 128 per chunk");
        let back: Vec<PageId> = chunks
            .iter()
            .flat_map(|c| decode_free_list_chunk(c).unwrap())
            .collect();
        assert_eq!(back, free);
        assert!(encode_free_list(&[]).unwrap().is_empty());
    }

    #[test]
    fn test_payload_with_an_unknown_version_is_refused() {
        let bytes = write_versioned(99, |out| out.extend_from_slice(&[1, 2, 3]));
        for err in [
            decode_generator_entry(&bytes).unwrap_err(),
            decode_free_list_chunk(&bytes).unwrap_err(),
            decode_catalog_entry(&bytes).map(|_| ()).unwrap_err(),
        ] {
            assert!(err.to_string().contains("version 99"), "got {err}");
        }
    }

    #[test]
    fn test_kind_indexes_are_dense_and_distinct() {
        let idx: Vec<usize> = SystemKind::ALL.iter().map(|k| k.index()).collect();
        assert_eq!(idx, vec![0, 1, 2]);
    }
}
