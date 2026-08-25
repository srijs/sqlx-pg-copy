use sqlx::error::BoxDynError;

/// This crate's [`Result`](std::result::Result), defaulting to [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error from building or running a copy.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A row had a different number of fields than the copy has columns.
    #[error("copy row {row} wrote {written} fields, and the copy has {columns} columns")]
    FieldCount {
        /// Which row, counting from zero.
        row: u64,
        /// How many columns the copy declares.
        columns: usize,
        /// Fields written, or for an over-long row the index it was refused
        /// at.
        written: usize,
    },

    /// The statement asks for a textual format, which is unsupported.
    #[error("the copy is expecting a textual format, and this writes binary")]
    Textual,

    /// A value failed to encode itself into the row.
    #[error("encoding field {column} of type {type_name}: {source}")]
    Encode {
        /// Which field of the row, counting from zero.
        column: usize,
        /// The Postgres type the value reports, to tell similar rows apart.
        type_name: String,
        /// What the `Encode` implementation returned.
        source: BoxDynError,
    },

    /// A field's encoding is too long for the format's `i32` length prefix.
    #[error("field {column} encodes to {len} bytes, and the limit is {}", i32::MAX)]
    FieldSize {
        /// Which field of the row, counting from zero.
        column: usize,
        /// How long the encoded value came out.
        len: usize,
    },

    /// The copy has more columns than a binary row's `i16` field count holds.
    #[error(
        "a binary copy row holds at most {} columns, and this one has {columns}",
        i16::MAX
    )]
    ColumnLimit {
        /// How many columns the copy declares.
        columns: usize,
    },

    /// The driver or server failed, at any point in the copy.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}
