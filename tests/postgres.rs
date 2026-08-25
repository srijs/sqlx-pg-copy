//! The encoder's claims, checked against a real server.
//!
//! Postgres is the only authority on whether the framing is right, so these
//! write rows and read them back instead of comparing bytes. Set
//! `DATABASE_URL` to point them at a server.

use anyhow::{Context as _, Result};
use proptest::prelude::*;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo};
use sqlx::{Connection as _, Encode, PgConnection, Postgres, Row as _, Type};
use sqlx_pg_copy::{CopyIn, Error};

/// The columns every case copies into, in the order rows write them.
const COLUMNS: &str = "(id, name, blob, tags, note)";

/// A row of the table, on both sides of the round trip.
#[derive(Debug, PartialEq, Eq)]
struct Sample {
    id: i64,
    name: String,
    blob: Vec<u8>,
    tags: Vec<i64>,
    note: Option<String>,
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost:5432/postgres".to_string())
}

fn copy_statement() -> String {
    format!("COPY sample {COLUMNS} FROM STDIN WITH (FORMAT binary)")
}

/// A connection with a temp table `sample`, dropped when it closes.
async fn connect() -> Result<PgConnection> {
    let mut conn = PgConnection::connect(&database_url())
        .await
        .context("connecting to the test database")?;
    sqlx::query(
        "CREATE TEMP TABLE sample (\
             id bigint PRIMARY KEY, \
             name text NOT NULL, \
             blob bytea NOT NULL, \
             tags bigint[] NOT NULL, \
             note text)",
    )
    .execute(&mut conn)
    .await?;
    Ok(conn)
}

async fn read_back(conn: &mut PgConnection) -> Result<Vec<Sample>> {
    let rows = sqlx::query("SELECT id, name, blob, tags, note FROM sample ORDER BY id")
        .fetch_all(conn)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(Sample {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                blob: row.try_get("blob")?,
                tags: row.try_get("tags")?,
                note: row.try_get("note")?,
            })
        })
        .collect()
}

#[tokio::test]
async fn bytea_is_the_bytes_themselves() {
    let mut conn = connect().await.unwrap();
    let blob: Vec<u8> = (0..=255).collect();

    let statement = copy_statement();
    let mut copy = CopyIn::begin(&mut conn, &statement).await.unwrap();
    copy.write_row(|row| {
        row.push_value(&1_i64)?;
        row.push_value(&"raw")?;
        // `raw` and `value` have to agree for a bytea, since a bytea's binary
        // encoding is nothing but its own bytes.
        row.push_raw(&blob)?;
        row.push_value(&Vec::<i64>::new())?;
        row.push_value(&None::<String>)
    })
    .await
    .unwrap();
    copy.finish().await.unwrap();

    let stored = read_back(&mut conn).await.unwrap();
    assert_eq!(stored.first().map(|sample| &sample.blob), Some(&blob));
}

#[tokio::test]
async fn a_row_of_empty_values_round_trips() {
    // The narrowest row the schema allows. A null and a zero-length value
    // differ only in the length prefix, which makes this the easiest case to
    // get wrong; a generated case found it once, so it is pinned here.
    let mut conn = connect().await.unwrap();
    let statement = copy_statement();
    let mut copy = CopyIn::begin(&mut conn, &statement).await.unwrap();
    copy.write_row(|row| {
        row.push_value(&0_i64)?;
        row.push_value(&"")?;
        row.push_value(&Vec::<u8>::new())?;
        row.push_value(&Vec::<i64>::new())?;
        row.push_value(&None::<String>)
    })
    .await
    .unwrap();
    assert_eq!(copy.finish().await.unwrap(), 1);

    assert_eq!(
        read_back(&mut conn).await.unwrap(),
        vec![Sample {
            id: 0,
            name: String::new(),
            blob: Vec::new(),
            tags: Vec::new(),
            note: None,
        }]
    );
}

#[tokio::test]
async fn a_streaming_copy_sends_as_it_fills() {
    let mut conn = connect().await.unwrap();
    let statement = copy_statement();
    let mut copy = CopyIn::begin(&mut conn, &statement)
        .await
        .unwrap()
        .flush_at(64);
    for id in 0..1000_i64 {
        copy.write_row(|row| {
            row.push_value(&id)?;
            row.push_value(&"streamed")?;
            row.push_raw(&[])?;
            row.push_value(&vec![id])?;
            row.push_value(&None::<String>)
        })
        .await
        .unwrap();
    }
    assert_eq!(copy.finish().await.unwrap(), 1000);

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sample")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1000);
}

#[tokio::test]
async fn the_column_count_comes_from_the_server() {
    let mut conn = connect().await.unwrap();
    let statement = copy_statement();
    let mut copy = CopyIn::begin(&mut conn, &statement).await.unwrap();
    let outcome = copy.write_row(|row| row.push_value(&1_i64)).await;
    assert!(
        matches!(outcome, Err(Error::FieldCount { columns: 5, .. })),
        "want a field-count error for the table's five columns, got {outcome:?}"
    );
    copy.abort("done with this one").await.unwrap();
}

#[tokio::test]
async fn a_textual_statement_is_refused_before_any_row_is_sent() {
    let mut conn = connect().await.unwrap();
    let statement = format!("COPY sample {COLUMNS} FROM STDIN WITH (FORMAT text)");
    let refused = match CopyIn::begin(&mut conn, &statement).await {
        Ok(copy) => {
            copy.abort("the statement asked for text after all")
                .await
                .unwrap();
            None
        }
        Err(err) => Some(err),
    };
    assert!(
        matches!(refused, Some(Error::Textual)),
        "want a format error, got {refused:?}"
    );

    // The abort sent on refusal is what leaves the connection usable, and a
    // query is the only way to check.
    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(one, 1);
}

#[tokio::test]
async fn what_the_server_refuses_arrives_as_the_driver_error() {
    let mut conn = connect().await.unwrap();
    let statement = copy_statement();
    let mut copy = CopyIn::begin(&mut conn, &statement).await.unwrap();
    for _ in 0..2 {
        copy.write_row(|row| {
            row.push_value(&1_i64)?;
            row.push_value(&"the same id twice")?;
            row.push_raw(&[])?;
            row.push_value(&Vec::<i64>::new())?;
            row.push_value(&None::<String>)
        })
        .await
        .unwrap();
    }

    let outcome = copy.finish().await;
    assert!(
        matches!(outcome, Err(Error::Sqlx(_))),
        "want the driver's error for a duplicate key, got {outcome:?}"
    );
}

/// A type Postgres knows by name rather than by OID, as
/// `#[derive(sqlx::Type)]` with a `type_name` produces.
#[derive(Debug)]
enum Role {
    Admin,
    Guest,
}

impl Type<Postgres> for Role {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("role")
    }
}

impl PgHasArrayType for Role {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_role")
    }
}

impl Encode<'_, Postgres> for Role {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend_from_slice(match self {
            Self::Admin => b"admin",
            Self::Guest => b"guest",
        });
        Ok(IsNull::No)
    }
}

#[tokio::test]
async fn a_type_the_server_knows_only_by_name_copies_anyway() {
    let mut conn = PgConnection::connect(&database_url()).await.unwrap();
    // The enum outlives the session, so a re-run finds it already there.
    let _ = sqlx::query("CREATE TYPE role AS ENUM ('admin', 'guest')")
        .execute(&mut conn)
        .await;
    sqlx::query("CREATE TEMP TABLE named (one role, many role[])")
        .execute(&mut conn)
        .await
        .unwrap();

    // `sqlx` writes a zero OID for an element type it knows only by name,
    // meaning to patch it from a prepared statement's parameters. A copy has
    // none, but succeeds anyway: `COPY` already knows its column types.
    let statement = "COPY named (one, many) FROM STDIN WITH (FORMAT binary)";
    let mut copy = CopyIn::begin(&mut conn, statement).await.unwrap();
    copy.write_row(|row| {
        row.push_value(&Role::Admin)?;
        row.push_value(&vec![Role::Admin, Role::Guest])
    })
    .await
    .unwrap();
    copy.finish().await.unwrap();

    let (one, many): (String, Vec<String>) =
        sqlx::query_as("SELECT one::text, many::text[] FROM named")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(one, "admin");
    assert_eq!(many, vec!["admin".to_string(), "guest".to_string()]);
}

/// A `text` value: any characters except NUL, which Postgres will not store.
fn a_string() -> impl Strategy<Value = String> {
    prop::collection::vec(
        any::<char>().prop_filter("not a NUL", |c| *c != '\0'),
        0..48,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

/// A row with the id left to the caller: it is the primary key, so rows are
/// numbered rather than generated.
fn a_sample() -> impl Strategy<Value = Sample> {
    (
        a_string(),
        prop::collection::vec(any::<u8>(), 0..64),
        prop::collection::vec(any::<i64>(), 0..8),
        prop::option::of(a_string()),
    )
        .prop_map(|(name, blob, tags, note)| Sample {
            id: 0,
            name,
            blob,
            tags,
            note,
        })
}

/// Report an error as a case failure rather than a panic.
fn failed(err: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(err.to_string())
}

proptest! {
    // Every case is a copy and a read back, so the count is set by what a
    // server will sit through rather than by what proptest would like.
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Whatever the rows are, they come back as they went in.
    ///
    /// No specific values are chosen, because this crate only frames them: a
    /// field is a length plus whatever `Encode` produced. What matters is the
    /// shape — empty fields, nulls, varying widths — so that is generated.
    #[test]
    fn any_rows_round_trip(generated in prop::collection::vec(a_sample(), 1..12)) {
        let samples: Vec<Sample> = generated
            .into_iter()
            .enumerate()
            .map(|(index, sample)| Sample {
                id: i64::try_from(index).expect("a small index"),
                ..sample
            })
            .collect();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(failed)?;
        let read = runtime.block_on(async {
            let mut conn = connect().await.map_err(failed)?;
            let statement = copy_statement();
            let mut copy = CopyIn::begin(&mut conn, &statement)
                .await
                .map_err(failed)?;
            for sample in &samples {
                copy.write_row(|row| {
                    row.push_value(&sample.id)?;
                    row.push_value(&sample.name)?;
                    row.push_value(&sample.blob)?;
                    row.push_value(&sample.tags)?;
                    row.push_value(&sample.note)
                })
                .await
                .map_err(failed)?;
            }
            let copied = copy.finish().await.map_err(failed)?;
            prop_assert_eq!(copied, samples.len() as u64);
            read_back(&mut conn).await.map_err(failed)
        })?;

        prop_assert_eq!(read, samples);
    }
}
