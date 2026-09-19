//! The small versioned envelope every persisted SQL-layer row shares
//! (persistence versioning Stages 7-8): `[0x00][u16 LE version][body]`.
//!
//! The leading 0x00 is what tells an enveloped row from one written before
//! the envelope existed. Every such legacy row is bare postcard whose FIRST
//! byte is a length varint for a non-empty name (a table name, a schema
//! name), which is 0x00 only for an EMPTY name — and each writer refuses to
//! encode an empty name, and no earlier build could create one. So "first
//! byte 0x00" means enveloped and anything else means legacy, with no
//! guessing. A new kind of row may use this only if its first legacy byte
//! has the same property; otherwise it needs its own discriminator.

use crate::error::SchemaError;

pub(crate) enum Opened<'a> {
    /// A row with no envelope: the legacy layout, i.e. what version 1's
    /// body is for every kind of row that has one.
    Legacy(&'a [u8]),
    Versioned { version: u16, body: &'a [u8] },
}

pub(crate) fn seal(version: u16, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + body.len());
    out.push(0x00);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Splits a stored row into legacy-or-versioned. `what` names the kind of
/// row for error messages. Never panics on short input.
pub(crate) fn open<'a>(bytes: &'a [u8], what: &str) -> Result<Opened<'a>, SchemaError> {
    match bytes.first() {
        None => Err(SchemaError::InternalSchemaError(format!("{what} row is empty"))),
        Some(0x00) => {
            let tag: [u8; 2] = bytes
                .get(1..3)
                .and_then(|t| t.try_into().ok())
                .ok_or_else(|| {
                    SchemaError::InternalSchemaError(format!(
                        "{what} row is {} byte(s), too short for its version tag",
                        bytes.len()
                    ))
                })?;
            Ok(Opened::Versioned {
                version: u16::from_le_bytes(tag),
                body: &bytes[3..],
            })
        }
        Some(_) => Ok(Opened::Legacy(bytes)),
    }
}

pub(crate) fn unsupported(what: &str, version: u16) -> SchemaError {
    SchemaError::InternalSchemaError(format!(
        "unsupported {what} row version {version} — this database may have been written by a \
         newer, deprecated, or unrecognized build"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seal_then_open_round_trips() {
        let row = seal(7, b"body");
        match open(&row, "x").unwrap() {
            Opened::Versioned { version, body } => assert_eq!((version, body), (7, &b"body"[..])),
            Opened::Legacy(_) => panic!("a sealed row must open as versioned"),
        }
    }

    #[test]
    fn test_a_row_not_starting_with_zero_is_legacy() {
        assert!(matches!(open(&[5, b'h'], "x").unwrap(), Opened::Legacy(_)));
    }

    #[test]
    fn test_short_and_empty_rows_are_errors_not_panics() {
        assert!(open(&[], "x").is_err());
        assert!(open(&[0x00], "x").is_err());
        assert!(open(&[0x00, 1], "x").is_err());
        assert!(matches!(
            open(&[0x00, 1, 0], "x").unwrap(),
            Opened::Versioned { version: 1, body: [] }
        ));
    }
}
