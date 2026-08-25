use std::fmt;

use sqlx::PgConnection;
use sqlx::postgres::PgCopyIn;

use crate::encode::{Encoder, Row};
use crate::{Error, Result};

/// Default buffer size at which [`CopyIn`] sends.
const FLUSH_AT: usize = 1 << 20;

/// A `COPY ... FROM STDIN WITH (FORMAT binary)` in progress.
///
/// Holds the connection until the copy ends. Rows are buffered and sent once
/// the buffer reaches [`flush_at`](Self::flush_at).
///
/// # Examples
///
/// ```no_run
/// # use sqlx_pg_copy::CopyIn;
/// # async fn stream(conn: &mut sqlx::PgConnection) -> Result<(), sqlx_pg_copy::Error> {
/// let statement = "COPY users (id) FROM STDIN WITH (FORMAT binary)";
/// let mut copy = CopyIn::begin(conn, statement).await?;
/// for id in 0..1_000_i64 {
///     copy.write_row(|row| row.push_value(&id)).await?;
/// }
///
/// let copied = copy.finish().await?;
/// assert_eq!(copied, 1_000);
/// # Ok(()) }
/// ```
pub struct CopyIn<'c> {
    copy: PgCopyIn<&'c mut PgConnection>,
    encoder: Encoder,
    flush_at: usize,
}

impl fmt::Debug for CopyIn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyIn")
            .field("flush_at", &self.flush_at)
            .finish_non_exhaustive()
    }
}

impl<'c> CopyIn<'c> {
    /// Run `statement` and put the connection into copy mode.
    ///
    /// # Errors
    ///
    /// [`Error::Textual`] if the statement asks for a textual format.
    /// [`Error::ColumnLimit`] if the copy is wider than a row can be.
    /// [`Error::Sqlx`] if there is an error on the underlying `sqlx` connection.
    pub async fn begin(conn: &'c mut PgConnection, statement: &str) -> Result<Self> {
        let copy = begin_checked(conn, statement).await?;
        // Abort here rather than failing on the first row, so the caller gets
        // a usable connection back.
        let encoder = match Encoder::new(copy.num_columns()) {
            Ok(encoder) => encoder,
            Err(err) => {
                copy.abort("the copy has more columns than a row can hold")
                    .await?;
                return Err(err);
            }
        };
        Ok(Self {
            copy,
            encoder,
            flush_at: FLUSH_AT,
        })
    }

    /// Send once `bytes` have been buffered, instead of the default megabyte.
    #[must_use]
    pub fn flush_at(mut self, bytes: usize) -> Self {
        self.flush_at = bytes;
        self
    }

    /// Append a row, filled in by `write`, sending if the buffer is full.
    ///
    /// # Errors
    ///
    /// Whatever `write` returns, [`Error::FieldCount`], or [`Error::Sqlx`].
    pub async fn write_row<E>(
        &mut self,
        write: impl FnOnce(&mut Row<'_>) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<Error>,
    {
        self.encoder.write_row(write)?;
        // Inlined rather than in its own `async fn`: this is false for all but
        // one row per buffer, and the extra `await` measured ~10% per row.
        if self.encoder.buffer().len() >= self.flush_at {
            self.flush().await.map_err(E::from)?;
        }
        Ok(())
    }

    /// End the copy, returning the row count the server reports.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlx`] if the server rejects the data or there is an error
    /// on the connection.
    pub async fn finish(mut self) -> Result<u64> {
        self.encoder.seal();
        self.flush().await?;
        Ok(self.copy.finish().await?)
    }

    /// Abandon the copy, discarding all its rows.
    ///
    /// # Errors
    ///
    /// [`Error::Sqlx`] if the server answers with anything but the expected
    /// abort error.
    pub async fn abort(self, message: impl Into<String>) -> Result<()> {
        Ok(self.copy.abort(message).await?)
    }

    async fn flush(&mut self) -> Result<()> {
        let buffer = self.encoder.buffer();
        if buffer.is_empty() {
            return Ok(());
        }
        self.copy.send(buffer.as_slice()).await?;
        // Replaces the buffer rather than clearing it; see `Encoder::reset`.
        self.encoder.reset();
        Ok(())
    }
}

/// Open the copy, checking the server expects binary.
async fn begin_checked<'c>(
    conn: &'c mut PgConnection,
    statement: &str,
) -> Result<PgCopyIn<&'c mut PgConnection>> {
    let copy = conn.copy_in_raw(statement).await?;
    if copy.is_textual() {
        copy.abort("the payload is binary and the statement is not")
            .await?;
        return Err(Error::Textual);
    }
    Ok(copy)
}
