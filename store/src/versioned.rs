//! Shared, hand-written version-tag envelope for persisted formats.
//!
//! Policy (see the persistence-versioning plan): every release must be able
//! to open a file written by any previous release, unless a version has
//! been deliberately deprecated. Nothing here enforces that by itself — it
//! just gives every type the same small, explicit vocabulary for declaring
//! "here is my version, here is my body" so each type's own encode/decode
//! can dispatch on it instead of every type reinventing its own scheme (or,
//! worse, relying on `#[derive(Serialize)]`'s declaration-order-dependent
//! wire format with no version at all).

use crate::error::StoreError;

/// Prepends a fixed 2-byte little-endian version tag ahead of `body`'s own
/// bytes. Fixed-width (not a varint) so the tag's own byte width never
/// depends on the version number.
pub(crate) fn write_versioned(version: u16, body: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&version.to_le_bytes());
    body(&mut out);
    out
}

/// Splits off the fixed 2-byte version tag written by `write_versioned`,
/// returning it alongside the remaining bytes. Bounds-checked (not a raw
/// slice index): this reads bytes straight off disk, so a truncated buffer
/// must surface as an `Err`, not a panic.
pub(crate) fn read_version_tag(bytes: &[u8]) -> Result<(u16, &[u8]), StoreError> {
    let (tag, rest) = bytes.split_at_checked(2).ok_or_else(|| {
        StoreError::Corruption(format!(
            "need 2 byte(s) for a version tag, buffer is {} byte(s)",
            bytes.len()
        ))
    })?;
    Ok((u16::from_le_bytes(tag.try_into().unwrap()), rest))
}

/// Shared error for a decode's `other => ...` arm: a version tag was read
/// successfully, but no decode branch recognizes it — either a file from a
/// future build, or one from a version that was deliberately deprecated.
pub(crate) fn unsupported_version(type_name: &str, version: u16) -> StoreError {
    StoreError::Corruption(format!(
        "unsupported {type_name} version {version} — this file may have been written by a \
         newer, deprecated, or unrecognized build"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let bytes = write_versioned(3, |out| out.extend_from_slice(b"hello"));
        let (version, rest) = read_version_tag(&bytes).unwrap();
        assert_eq!(version, 3);
        assert_eq!(rest, b"hello");
    }

    #[test]
    fn test_tag_is_fixed_width_regardless_of_value() {
        let small = write_versioned(1, |_| {});
        let large = write_versioned(u16::MAX, |_| {});
        assert_eq!(small.len(), 2);
        assert_eq!(large.len(), 2);
        let (version, rest) = read_version_tag(&large).unwrap();
        assert_eq!(version, u16::MAX);
        assert!(rest.is_empty());
    }

    #[test]
    fn test_truncated_input_errors() {
        assert!(read_version_tag(&[]).is_err());
        assert!(read_version_tag(&[1]).is_err());
    }

    #[test]
    fn test_unsupported_version_error_names_type_and_version() {
        let err = unsupported_version("Widget", 99);
        let msg = err.to_string();
        assert!(msg.contains("Widget"), "message was: {msg}");
        assert!(msg.contains("99"), "message was: {msg}");
    }
}
