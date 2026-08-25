use sqlx::encode::IsNull;
use sqlx::postgres::PgArgumentBuffer;
use sqlx::{Encode, Postgres, Type, TypeInfo as _};

use crate::{Error, Result};

/// The signature every binary payload opens with.
const SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

/// The flags word and the header extension length, both unused, both zero.
const HEADER_FIELDS: [u8; 8] = [0; 8];

/// The trailer: a field count no row can have.
const TRAILER: [u8; 2] = (-1_i16).to_be_bytes();

/// A null field: a length prefix of -1 and no data.
const NULL_FIELD: [u8; 4] = (-1_i32).to_be_bytes();

/// The payload a copy builds, row by row.
///
/// This is [`CopyIn`](crate::CopyIn)'s buffer: rows go in via
/// [`write_row`](Self::write_row), and the copy reads them out of
/// [`buffer`](Self::buffer) when it sends. Not public, because building a
/// payload to keep rather than send is outside what this crate does.
#[derive(Debug)]
pub(crate) struct Encoder {
    /// A `PgArgumentBuffer` so values can be encoded straight into it.
    ///
    /// A field's length is only known after the value is written, but the
    /// format puts it first, so the four bytes are written as a null and
    /// overwritten afterwards.
    buf: PgArgumentBuffer,
    columns: usize,
    /// The column count every row opens with, pre-converted: it is the same
    /// for every row, and converting it per row was measurable.
    count: [u8; 2],
    rows: u64,
}

impl Encoder {
    /// A payload holding just the header, for a copy of `columns` columns.
    ///
    /// # Errors
    ///
    /// [`Error::ColumnLimit`] if a row cannot express that many fields.
    pub(crate) fn new(columns: usize) -> Result<Self> {
        let Ok(count) = i16::try_from(columns) else {
            return Err(Error::ColumnLimit { columns });
        };
        let mut encoder = Self {
            buf: PgArgumentBuffer::default(),
            columns,
            count: count.to_be_bytes(),
            rows: 0,
        };
        encoder.buf.reserve(SIGNATURE.len() + HEADER_FIELDS.len());
        encoder.buf.extend_from_slice(SIGNATURE);
        encoder.buf.extend_from_slice(&HEADER_FIELDS);
        Ok(encoder)
    }

    /// Append a row, filled in by `write`.
    ///
    /// A row that writes the wrong number of fields, or fails partway, is
    /// truncated back out of the buffer.
    ///
    /// # Errors
    ///
    /// Whatever `write` returns, or [`Error::FieldCount`].
    pub(crate) fn write_row<E>(
        &mut self,
        write: impl FnOnce(&mut Row<'_>) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<Error>,
    {
        let start = self.buf.len();
        self.buf.extend_from_slice(&self.count);
        let mut row = Row {
            buf: &mut self.buf,
            columns: self.columns,
            written: 0,
            index: self.rows,
        };
        let outcome = write(&mut row).and_then(|()| row.finish().map_err(E::from));
        match outcome {
            Ok(()) => {
                self.rows += 1;
                Ok(())
            }
            Err(err) => {
                self.buf.truncate(start);
                Err(err)
            }
        }
    }

    /// The bytes written so far. A copy sends these, then calls
    /// [`reset`](Self::reset).
    pub(crate) fn buffer(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    /// Replace the buffer with an empty one of the same capacity.
    ///
    /// Must be a new buffer, not a cleared one. For each type it knows by name
    /// rather than OID, `sqlx` records an offset to patch later against a
    /// prepared statement's parameters. A copy has no parameters, so those
    /// entries are never read — and clearing goes through `Deref` to
    /// `Vec::clear`, which empties the bytes but leaves them. They would
    /// otherwise grow for the whole copy.
    pub(crate) fn reset(&mut self) {
        let capacity = self.buf.capacity();
        self.buf = PgArgumentBuffer::default();
        self.buf.reserve(capacity);
    }

    /// Append the trailer that ends the payload. Called once, before the
    /// final send.
    pub(crate) fn seal(&mut self) {
        self.buf.extend_from_slice(&TRAILER);
    }
}

/// A row being filled in, field by field in column order.
#[derive(Debug)]
pub struct Row<'a> {
    buf: &'a mut PgArgumentBuffer,
    columns: usize,
    written: usize,
    /// Which row of the copy this is, for error messages.
    index: u64,
}

impl Row<'_> {
    /// Append a null field.
    ///
    /// # Errors
    ///
    /// [`Error::FieldCount`] if the row already holds every column.
    pub fn push_null(&mut self) -> Result<()> {
        self.open()?;
        self.buf.extend_from_slice(&NULL_FIELD);
        Ok(())
    }

    /// Append `value` by binary encoding via `Encode`.
    ///
    /// # Errors
    ///
    /// [`Error::Encode`] if the value refuses to encode, [`Error::FieldSize`]
    /// if it outgrows a length prefix, or [`Error::FieldCount`] past the last.
    pub fn push_value<'q, T>(&mut self, value: &'q T) -> Result<()>
    where
        T: Encode<'q, Postgres> + Type<Postgres>,
    {
        let column = self.open()?;
        // Write the length as -1 up front. If the value turns out to be null
        // that is already correct; otherwise it is overwritten below.
        let start = self.buf.len();
        self.buf.extend_from_slice(&NULL_FIELD);
        match value.encode_by_ref(&mut *self.buf) {
            // An `Encode` returning `Yes` must not have written anything,
            // and -1 is already the right length.
            Ok(IsNull::Yes) => self.buf.truncate(start + NULL_FIELD.len()),
            Ok(IsNull::No) => {
                let written = self.buf.len() - start - NULL_FIELD.len();
                let Ok(len) = i32::try_from(written) else {
                    self.buf.truncate(start);
                    return Err(Error::FieldSize {
                        column,
                        len: written,
                    });
                };
                // The four bytes were written at `start` and only appends
                // have happened since, so they are always present. If they
                // were not, leaving the -1 would silently store a null, so
                // fail the row instead.
                let Some(prefix) = self
                    .buf
                    .get_mut(start..)
                    .and_then(<[u8]>::first_chunk_mut::<{ NULL_FIELD.len() }>)
                else {
                    return Err(self.lost_prefix(start, column, written));
                };
                *prefix = len.to_be_bytes();
            }
            Err(source) => {
                self.buf.truncate(start);
                return Err(Error::Encode {
                    column,
                    type_name: T::type_info().name().to_owned(),
                    source,
                });
            }
        }
        Ok(())
    }

    /// Append a field from bytes that are already encoded.
    ///
    /// # Errors
    ///
    /// [`Error::FieldSize`] if `bytes` is larger than a length prefix holds,
    /// and [`Error::FieldCount`] if the row already holds every column.
    pub fn push_raw(&mut self, bytes: &[u8]) -> Result<()> {
        let column = self.open()?;
        push_field(self.buf, bytes, column)
    }

    /// Fail a field whose length prefix has gone missing.
    ///
    /// Out of line because it cannot happen, and because inlining it made
    /// `push_value` too large to inline into a caller's row loop, costing
    /// ~8% on wide rows.
    #[cold]
    #[inline(never)]
    fn lost_prefix(&mut self, start: usize, column: usize, len: usize) -> Error {
        self.buf.truncate(start);
        Error::FieldSize { column, len }
    }

    /// Start the next field, returning which column it is.
    fn open(&mut self) -> Result<usize> {
        if self.written >= self.columns {
            return Err(self.field_count(self.written + 1));
        }
        let column = self.written;
        self.written += 1;
        Ok(column)
    }

    /// End the row. Only the field count needs checking; a binary row states
    /// its own length up front.
    fn finish(self) -> Result<()> {
        if self.written != self.columns {
            return Err(self.field_count(self.written));
        }
        Ok(())
    }

    fn field_count(&self, written: usize) -> Error {
        Error::FieldCount {
            row: self.index,
            columns: self.columns,
            written,
        }
    }
}

/// Append a field: its length, then its encoded bytes.
fn push_field(buf: &mut Vec<u8>, bytes: &[u8], column: usize) -> Result<()> {
    let Ok(len) = i32::try_from(bytes.len()) else {
        return Err(Error::FieldSize {
            column,
            len: bytes.len(),
        });
    };
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Byte-for-byte checks of what the encoder writes.
    //!
    //! These are claims about the wire format, so they need no server;
    //! `tests/postgres.rs` confirms them against a real one.

    use super::Encoder;
    use crate::Error;

    /// The 19 bytes a payload opens with.
    const HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";

    /// The trailer that ends one.
    const TRAILER: &[u8] = b"\xff\xff";

    /// The complete payload, as a copy would send it.
    fn payload(mut encoder: Encoder) -> Vec<u8> {
        encoder.seal();
        encoder.buffer().clone()
    }

    /// The payload so far, as a copy would be holding it.
    fn buffered(encoder: &mut Encoder) -> Vec<u8> {
        encoder.buffer().clone()
    }

    #[test]
    fn it_opens_with_the_signature_and_ends_with_the_trailer() {
        let bytes = payload(Encoder::new(1).unwrap());
        assert_eq!(bytes.get(..HEADER.len()), Some(HEADER));
        assert_eq!(bytes.get(HEADER.len()..), Some(TRAILER));
    }

    #[test]
    fn it_writes_a_field_count_and_then_length_prefixed_fields() {
        let mut encoder = Encoder::new(3).unwrap();
        encoder
            .write_row(|row| {
                row.push_value(&1_i64)?;
                row.push_value(&None::<i64>)?;
                row.push_raw(&[0xaa, 0xbb])
            })
            .unwrap();
        let mut want = HEADER.to_vec();
        want.extend_from_slice(&3_i16.to_be_bytes());
        want.extend_from_slice(&8_i32.to_be_bytes());
        want.extend_from_slice(&1_i64.to_be_bytes());
        want.extend_from_slice(&(-1_i32).to_be_bytes());
        want.extend_from_slice(&2_i32.to_be_bytes());
        want.extend_from_slice(&[0xaa, 0xbb]);
        want.extend_from_slice(TRAILER);
        assert_eq!(payload(encoder), want);
    }

    #[test]
    fn it_encodes_a_value_the_way_a_bind_parameter_would() {
        let mut encoder = Encoder::new(1).unwrap();
        encoder.write_row(|row| row.push_value(&"héllo")).unwrap();
        let mut want = HEADER.to_vec();
        want.extend_from_slice(&1_i16.to_be_bytes());
        want.extend_from_slice(&6_i32.to_be_bytes());
        want.extend_from_slice("héllo".as_bytes());
        want.extend_from_slice(TRAILER);
        assert_eq!(payload(encoder), want);
    }

    #[test]
    fn a_short_row_is_an_error_and_leaves_nothing_behind() {
        let mut encoder = Encoder::new(2).unwrap();
        let outcome = encoder.write_row(|row| row.push_value(&1_i64));
        assert!(
            matches!(
                outcome,
                Err(Error::FieldCount {
                    row: 0,
                    columns: 2,
                    written: 1
                })
            ),
            "want a field-count error, got {outcome:?}"
        );
        assert_eq!(
            buffered(&mut encoder),
            HEADER,
            "the partial row is rolled back, leaving only the header"
        );
    }

    #[test]
    fn a_long_row_is_an_error_at_the_field_that_overflows() {
        let mut encoder = Encoder::new(1).unwrap();
        let outcome = encoder.write_row(|row| {
            row.push_value(&1_i64)?;
            row.push_value(&2_i64)
        });
        assert!(
            matches!(outcome, Err(Error::FieldCount { written: 2, .. })),
            "want a field-count error, got {outcome:?}"
        );
        assert_eq!(buffered(&mut encoder), HEADER);
    }

    #[test]
    fn a_copy_wider_than_a_row_can_say_is_refused() {
        let outcome = Encoder::new(40_000).map(|_| ());
        assert!(
            matches!(outcome, Err(Error::ColumnLimit { columns: 40_000 })),
            "want a column-limit error, got {outcome:?}"
        );
    }

    #[test]
    fn sealing_appends_the_trailer_and_nothing_else() {
        let mut encoder = Encoder::new(1).unwrap();
        encoder.write_row(|row| row.push_value(&7_i32)).unwrap();
        let unsealed = buffered(&mut encoder);
        let sealed = payload(encoder);
        assert_eq!(sealed.get(..unsealed.len()), Some(unsealed.as_slice()));
        assert_eq!(sealed.get(unsealed.len()..), Some(TRAILER));
    }
}
