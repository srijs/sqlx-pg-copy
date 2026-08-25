//! End to end, against a stub that implements only the wire protocol.
//!
//! Measures a whole copy: `sqlx` framing each flush into a `CopyData` message,
//! the socket, and the messages that open and close a copy. A real server is
//! left out because its ingest is slow enough to hide everything else.
//!
//! The stub answers a startup with `AuthenticationOk`, a `Query` with
//! `CopyInResponse`, counts and drops `CopyData`, and answers `CopyDone` with
//! `CommandComplete` and `ReadyForQuery`.
//!
//! Each shape is measured twice: `copy` runs rows through this crate, and
//! `driver` pushes the same byte count through `sqlx`'s `copy_in_raw` with no
//! rows built. The second is the floor, so the difference is this crate's
//! cost.
//!
//! ```text
//! cargo bench                        # both, with statistics
//! cargo bench -- --save-baseline now # keep it to compare a change against
//! cargo bench -- --baseline now      # and the change, against that
//! ```
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "the manifest's bans exist to protect callers, and a benchmark has \
              none; criterion_group! also generates an undocumented function"
)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo};
use sqlx::{Connection as _, Encode, PgConnection, Postgres, Type};
use sqlx_pg_copy::{CopyIn, Error, Row};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

/// Rows per copy, which is one iteration. Enough that most shapes fill the
/// buffer and send more than once, since a copy that never flushes is not
/// representative.
const ROWS: usize = 50_000;

/// Bytes moved per sweep iteration, so every field size does equal work.
const SWEEP_BYTES: usize = 4 << 20;

/// This crate's flush threshold; the floor sends the same sized pieces.
const FLUSH_AT: usize = 1 << 20;

/// The statement. The stub answers the same regardless of its text.
const STATEMENT: &str = "COPY t FROM STDIN WITH (FORMAT binary)";

/// One buffer for the floor to send, built once so the measurement does not
/// include an allocation.
static PIECE: LazyLock<Vec<u8>> = LazyLock::new(|| vec![0x5a_u8; FLUSH_AT]);

/// A type Postgres knows by name rather than by OID. `sqlx` records an offset
/// for these that the copy never uses.
#[derive(Debug)]
struct Named(i32);

impl Type<Postgres> for Named {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("named")
    }
}

impl PgHasArrayType for Named {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_named")
    }
}

impl Encode<'_, Postgres> for Named {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend_from_slice(&self.0.to_be_bytes());
        Ok(IsNull::No)
    }
}

fn benches(c: &mut Criterion) {
    let runtime = Runtime::new().expect("a runtime");

    // Leaked so the closures below stay `Copy`, so each iteration's async
    // block can take one.
    let blob: &[u8] = Box::leak(vec![0x5a_u8; 4096].into_boxed_slice());
    let tags: &[i64] = Box::leak((0..8).collect::<Vec<i64>>().into_boxed_slice());
    let note: &Option<String> = Box::leak(Box::new(Some("a note of thirty bytes or so".into())));
    let named: &[Named] = Box::leak((0..8).map(Named).collect::<Vec<Named>>().into_boxed_slice());

    shape(c, &runtime, "4 x i64", 4, |row, id| {
        row.push_value(&id)?;
        row.push_value(&(id + 1))?;
        row.push_value(&(id + 2))?;
        row.push_value(&(id + 3))
    });
    shape(c, &runtime, "32 x i64", 32, |row, id| {
        for column in 0..32 {
            row.push_value(&(id + column))?;
        }
        Ok(())
    });
    shape(c, &runtime, "4 x text", 4, |row, _| {
        row.push_value(&"a short string")?;
        row.push_value(&"another one")?;
        row.push_value(&"and a third")?;
        row.push_value(&"the last")
    });
    shape(c, &runtime, "1 x bytea 4 KiB", 1, |row, _| {
        row.push_raw(blob)
    });
    shape(c, &runtime, "1 x bigint[8]", 1, |row, _| {
        row.push_value(&tags)
    });
    shape(c, &runtime, "4 x null", 4, |row, _| {
        row.push_null()?;
        row.push_null()?;
        row.push_null()?;
        row.push_null()
    });
    shape(c, &runtime, "1 x named[8]", 1, |row, _| {
        row.push_value(&named)
    });
    sweep(c, &runtime);
    shape(c, &runtime, "mixed", 5, |row, id| {
        row.push_value(&id)?;
        row.push_value(&"a short string")?;
        row.push_raw(&[0xde, 0xad, 0xbe, 0xef])?;
        row.push_value(&tags)?;
        row.push_value(note)
    });
}

/// How field size affects the two ways of writing one.
///
/// `push_raw` passes bytes through as they are; `push_value` runs them through
/// `sqlx`'s `Encode`. Both end in one copy into the payload, so this shows
/// what the per-field work costs beside that copy, and the size at which it
/// stops mattering.
///
/// Each size gets a row count that keeps the bytes moved roughly equal.
fn sweep(c: &mut Criterion, runtime: &Runtime) {
    for size in [64_usize, 1 << 10, 1 << 14, 1 << 18, 1 << 21] {
        let value: &[u8] = Box::leak(vec![0x5a_u8; size].into_boxed_slice());
        let text: &str = Box::leak(
            String::from_utf8(vec![b'x'; size])
                .expect("ascii is text")
                .into_boxed_str(),
        );
        let rows = (SWEEP_BYTES / size).max(4);

        sized(
            c,
            runtime,
            "bytea/push_raw",
            Some(size),
            1,
            rows,
            move |row, _| row.push_raw(value),
        );
        sized(
            c,
            runtime,
            "text/push_value",
            Some(size),
            1,
            rows,
            move |row, _| row.push_value(&text),
        );
    }
}

/// Measure one row shape, and the same bytes with no rows built.
fn shape<F>(c: &mut Criterion, runtime: &Runtime, name: &str, columns: usize, fill: F)
where
    F: Fn(&mut Row<'_>, i64) -> Result<(), Error> + Copy,
{
    sized(c, runtime, name, None, columns, ROWS, fill);
}

/// One shape, or one size of a shape when `size` is given.
fn sized<F>(
    c: &mut Criterion,
    runtime: &Runtime,
    name: &str,
    size: Option<usize>,
    columns: usize,
    rows: usize,
    fill: F,
) where
    F: Fn(&mut Row<'_>, i64) -> Result<(), Error> + Copy,
{
    let (addr, counted) = runtime.block_on(stub(columns, rows as u64));

    // One copy up front, to learn how many bytes this shape is worth.
    counted.store(0, Ordering::Relaxed);
    runtime.block_on(async {
        let mut conn = connect(addr).await;
        copy_rows(&mut conn, rows, fill).await;
        let _ = conn.close().await;
    });
    let bytes = counted.load(Ordering::Relaxed);

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(bytes));
    // Many benchmarks here, so the defaults would run for minutes.
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    let label = |half: &str| match size {
        Some(size) => BenchmarkId::new(half, size),
        None => BenchmarkId::new(half, "row"),
    };
    group.bench_function(label("copy"), |b| {
        b.to_async(runtime).iter_custom(move |iters| async move {
            let mut conn = connect(addr).await;
            let start = Instant::now();
            for _ in 0..iters {
                copy_rows(&mut conn, rows, fill).await;
            }
            let elapsed = start.elapsed();
            let _ = conn.close().await;
            elapsed
        });
    });
    group.bench_function(label("driver"), |b| {
        b.to_async(runtime).iter_custom(move |iters| async move {
            let mut conn = connect(addr).await;
            let start = Instant::now();
            for _ in 0..iters {
                send_bytes(&mut conn, bytes).await;
            }
            let elapsed = start.elapsed();
            let _ = conn.close().await;
            elapsed
        });
    });
    group.finish();
}

/// One copy of `rows` rows through this crate.
async fn copy_rows<F>(conn: &mut PgConnection, rows: usize, fill: F)
where
    F: Fn(&mut Row<'_>, i64) -> Result<(), Error>,
{
    let mut copy = CopyIn::begin(conn, STATEMENT).await.expect("a copy");
    for id in 0..rows {
        copy.write_row(|row| fill(row, id as i64))
            .await
            .expect("a row");
    }
    let counted = copy.finish().await.expect("the count");
    assert_eq!(counted, rows as u64, "the stub counts what it was told");
}

/// The same byte count through `sqlx` alone, in the same sized pieces, with
/// no rows built.
async fn send_bytes(conn: &mut PgConnection, bytes: u64) {
    let mut copy = conn.copy_in_raw(STATEMENT).await.expect("a copy");
    let mut sent = 0;
    while sent < bytes {
        let take = usize::try_from((bytes - sent).min(FLUSH_AT as u64)).expect("a piece");
        copy.send(&PIECE[..take]).await.expect("a piece");
        sent += take as u64;
    }
    copy.finish().await.expect("the count");
}

/// A connection to the stub.
async fn connect(addr: SocketAddr) -> PgConnection {
    let url = format!(
        "postgresql://bench@{}:{}/bench?sslmode=disable",
        addr.ip(),
        addr.port()
    );
    PgConnection::connect(&url).await.expect("the stub")
}

criterion_group!(copy_in, benches);
criterion_main!(copy_in);

/// A listener answering copies until the process ends, plus a running count
/// of the payload bytes it has received.
async fn stub(columns: usize, rows: u64) -> (SocketAddr, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let addr = listener.local_addr().expect("an address");
    let counted = Arc::new(AtomicU64::new(0));
    let counting = Arc::clone(&counted);
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let counting = Arc::clone(&counting);
            tokio::spawn(async move {
                serve(socket, columns, rows, &counting).await.ok();
            });
        }
    });
    (addr, counted)
}

/// Speak enough of the protocol to accept a copy, counting what arrives.
async fn serve(
    mut socket: TcpStream,
    columns: usize,
    rows: u64,
    counted: &AtomicU64,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;

    // The startup message has no type byte.
    let len = socket.read_u32().await? as usize;
    let mut startup = vec![0; len - 4];
    socket.read_exact(&mut startup).await?;
    socket.write_all(&authentication_ok()).await?;
    socket.write_all(&ready_for_query()).await?;
    socket.flush().await?;

    let mut body = Vec::new();
    loop {
        let Ok(kind) = socket.read_u8().await else {
            return Ok(());
        };
        let len = socket.read_u32().await? as usize - 4;
        body.resize(len, 0);
        socket.read_exact(&mut body[..len]).await?;
        match kind {
            b'Q' => {
                socket.write_all(&copy_in_response(columns)).await?;
                socket.flush().await?;
            }
            b'd' => {
                counted.fetch_add(len as u64, Ordering::Relaxed);
            }
            b'c' => {
                socket.write_all(&command_complete(rows)).await?;
                socket.write_all(&ready_for_query()).await?;
                socket.flush().await?;
            }
            b'X' => return Ok(()),
            other => panic!("the stub was sent a {:?} it does not know", other as char),
        }
    }
}

fn authentication_ok() -> [u8; 9] {
    let mut message = [0; 9];
    message[0] = b'R';
    message[1..5].copy_from_slice(&8_i32.to_be_bytes());
    message[5..9].copy_from_slice(&0_i32.to_be_bytes());
    message
}

fn ready_for_query() -> [u8; 6] {
    let mut message = [0; 6];
    message[0] = b'Z';
    message[1..5].copy_from_slice(&5_i32.to_be_bytes());
    message[5] = b'I';
    message
}

fn copy_in_response(columns: usize) -> Vec<u8> {
    let mut message = vec![b'G'];
    message.extend_from_slice(&((4 + 1 + 2 + 2 * columns) as i32).to_be_bytes());
    message.push(1); // binary
    message.extend_from_slice(&(columns as i16).to_be_bytes());
    for _ in 0..columns {
        message.extend_from_slice(&1_i16.to_be_bytes());
    }
    message
}

fn command_complete(rows: u64) -> Vec<u8> {
    let tag = format!("COPY {rows}\0");
    let mut message = vec![b'C'];
    message.extend_from_slice(&((4 + tag.len()) as i32).to_be_bytes());
    message.extend_from_slice(tag.as_bytes());
    message
}
