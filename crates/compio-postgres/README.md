# compio-postgres

A local PostgreSQL connection pool using compio TCP/Unix sockets, TLS and tasks.
The established `tokio-postgres` protocol implementation is compiled with its
runtime feature disabled. Tokio I/O traits and codecs adapt the protocol to
compio; no Tokio reactor or executor is created. Poll-based TLS adapters keep
the same pinned compio socket throughout PostgreSQL SSL negotiation.

```rust,no_run
use std::num::NonZeroU32;
use compio_postgres::Pool;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let pool = Pool::new("postgres://localhost/bot?sslmode=require", NonZeroU32::MIN)?;
let session = pool.get().await?;
let row = session.client().query_one("SELECT $1::BIGINT", &[&42_i64]).await?;
assert_eq!(row.get::<_, i64>(0), 42);
session.recycle();
# Ok(())
# }
```

`Pool::new` parses the URL. `Pool::get` establishes sessions lazily up to the
configured bound. Acquisition has a 30-second deadline covering permit wait,
connection setup and session validation, preserving the previous SQLx default. A lease owns both its protocol client and compio connection
driver. Drop an unfinished lease to close the session. Recycle it only after the
complete result has been consumed. Queries are never automatically retried.
The pool does not reset session settings on return. `prepare_cached` retains up
to 100 statements per session by default; `statement-cache-capacity` changes
that limit (zero disables caching).

The `native-tls` default feature uses the platform TLS backend; `rustls` selects
rustls with WebPKI roots. When both are enabled, native TLS takes precedence.
URL options `sslmode=disable|allow|prefer|require|verify-ca|verify-full`,
`sslrootcert` (`ssl-ca`), `sslcert` and `sslkey` configure TLS. Startup
`options` and `options[name]` retain SQLx URL syntax. Missing URL values use
the existing `PGHOST`, `PGHOSTADDR`, `PGPORT`, `PGUSER`, `PGDATABASE`,
`PGPASSWORD`, `PGAPPNAME`, `PGOPTIONS` and `PGSSL*` environment defaults;
passwords can come from `PGPASSFILE` or the usual passfile location. Inline
PEM in TLS environment settings is supported. The passfile parser and its
regression tests are adapted from SQLx under the included MIT license.

`prefer` and `require` encrypt without verifying the server certificate;
`allow` tries plaintext first, then TLS after a failed connection attempt.
This implements PostgreSQL semantics beyond the previous SQLx plaintext-only
`allow` implementation; successful plaintext connections behave the same.
`verify-ca` validates its issuer and `verify-full` also validates its name.
A build without either TLS feature accepts plaintext connections; a server
requiring TLS returns an error. Unix socket connections do not use TLS.

`channel_binding=require` returns an explicit unsupported-configuration error.
The adapter does not currently provide peer certificate bindings; `prefer` and `disable`
use plain SCRAM authentication, matching the previous SQLx backend.

A compio runtime must be running when acquiring or using sessions. Futures and
pools may contain local resources and do not require `Send` or `Sync`.
