# sqlx-pg-copy

[![crates.io](https://img.shields.io/crates/v/sqlx-pg-copy.svg)](https://crates.io/crates/sqlx-pg-copy)
[![docs.rs](https://docs.rs/sqlx-pg-copy/badge.svg)](https://docs.rs/sqlx-pg-copy)

_Binary `COPY ... FROM STDIN` encoder for [`sqlx`](https://github.com/launchbadge/sqlx)'s Postgres driver._

## Usage

```rust
use sqlx_pg_copy::{CopyIn, Error};

struct User {
    id: i64,
    name: String,
    email: Option<String>,
}

async fn copy_users(conn: &mut sqlx::PgConnection, users: &[User]) -> Result<u64, Error> {
    let statement = "COPY users (id, name, email) FROM STDIN WITH (FORMAT binary)";
    let mut copy = CopyIn::begin(conn, statement).await?;
    for user in users {
        copy.write_row(|row| {
            row.push_value(&user.id)?;
            row.push_value(&user.name)?;
            row.push_value(&user.email)
        })
        .await?;
    }
    copy.finish().await
}
```

## Not included

- Text and CSV formats: Focus is on efficient binary encoding leveraging `sqlx`'s `Encode` trait.
- `CopyOut`: Impossible to do on `sqlx` because the `Decode` trait is not usable outside `sqlx` itself.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
