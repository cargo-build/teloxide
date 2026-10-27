//! PostgreSQL protocol sessions driven exclusively by compio sockets and tasks.
//!
//! Dropping an unfinished lease closes its session. Call [`Lease::recycle`] only
//! after the complete result has been consumed to return a session to the pool.

#[cfg(any(feature = "native-tls", feature = "rustls"))]
mod tls;

mod config;
mod pgpass;

use std::{cell::RefCell, collections::VecDeque, fmt, io, num::NonZeroU32, time::Duration};

use async_lock::{Semaphore, SemaphoreGuard};
use compio::{
    io::{AsyncRead, AsyncWrite, compat::AsyncStream, util::Splittable},
    net::TcpStream,
    runtime::JoinHandle,
};
use tokio_postgres::{
    Config,
    config::{Host, LoadBalanceHosts, SslMode, TargetSessionAttrs},
};

use tokio_util::compat::FuturesAsyncReadCompatExt;

#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
use crate::config::Verification;
use crate::config::{TlsSettings, parse_config};

pub use tokio_postgres::{Client, Error, Row, Statement, types};

/// A bounded local pool of PostgreSQL sessions.
pub struct Pool {
    config: Config,
    tls: TlsSettings,
    idle: RefCell<Vec<Connection>>,
    permits: Semaphore,
    statement_cache_capacity: usize,
}

#[derive(Debug)]
struct Connection {
    client: Client,
    _driver: JoinHandle<()>,
    statements: RefCell<VecDeque<(String, Statement)>>,
}

/// A session which is closed on drop unless explicitly recycled.
#[derive(Debug)]
pub struct Lease<'a> {
    connection: Connection,
    pool: &'a Pool,
    _permit: SemaphoreGuard<'a>,
}

impl Pool {
    /// Resolves URL, environment and passfile options without opening a session.
    ///
    /// # Errors
    /// Returns an error for malformed URLs, TLS options or mismatched credentials.
    pub fn new(database_url: &str, max_connections: NonZeroU32) -> io::Result<Self> {
        let (config, tls, statement_cache_capacity) = parse_config(database_url)?;
        #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
        if tls.verification != Verification::None
            || tls.root.is_some()
            || tls.certificate.is_some()
            || tls.key.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "PostgreSQL TLS configuration requires a TLS feature",
            ));
        }
        Ok(Self {
            config,
            tls,
            idle: RefCell::new(Vec::new()),
            permits: Semaphore::new(max_connections.get() as usize),
            statement_cache_capacity,
        })
    }

    /// Acquires a session within 30 seconds, including waiting and connection setup.
    ///
    /// # Errors
    /// Returns connection, authentication or TLS errors without retrying queries.
    ///
    /// # Panics
    /// Panics if no compio runtime is active when a session is established.
    pub async fn get(&self) -> io::Result<Lease<'_>> {
        self.get_with_timeout(Duration::from_secs(30)).await
    }

    async fn get_with_timeout(&self, timeout: Duration) -> io::Result<Lease<'_>> {
        let acquire = async {
            let permit = self.permits.acquire().await;
            let idle =
                self.idle.borrow_mut().pop().filter(|connection| !connection.client.is_closed());
            let connection = match idle {
                Some(connection) => connection,
                None => self.connect().await?,
            };
            Ok(Lease { connection, pool: self, _permit: permit })
        };
        compio::time::timeout(timeout, acquire)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?
    }

    async fn connect(&self) -> io::Result<Connection> {
        let hosts = self.config.get_hosts();
        let hostaddrs = self.config.get_hostaddrs();
        let count = hosts.len().max(hostaddrs.len());
        if !hosts.is_empty() && !hostaddrs.is_empty() && hosts.len() != hostaddrs.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host and hostaddr counts differ",
            ));
        }
        let ports = self.config.get_ports();
        if ports.len() > 1 && ports.len() != count {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "host and port counts differ"));
        }
        let mut last_error =
            io::Error::new(io::ErrorKind::InvalidInput, "PostgreSQL host is missing");
        let mut indices: Vec<_> = (0..count).collect();
        if self.config.get_load_balance_hosts() == LoadBalanceHosts::Random {
            use rand::seq::SliceRandom;
            indices.shuffle(&mut rand::rng());
        }
        for index in indices {
            let port = ports.get(index).or_else(|| ports.first()).copied().unwrap_or(5432);
            let host = hostaddrs
                .get(index)
                .map(|address| Host::Tcp(address.to_string()))
                .or_else(|| hosts.get(index).cloned());
            let hostname = match hosts.get(index) {
                Some(Host::Tcp(hostname)) => Some(hostname.clone()),
                _ => hostaddrs.get(index).map(ToString::to_string),
            };
            let connect = async {
                let first = self
                    .connect_host(
                        host.as_ref(),
                        hostname.as_deref(),
                        port,
                        self.tls.plaintext_first,
                    )
                    .await;
                match first {
                    Err(_) if self.tls.plaintext_first => {
                        self.connect_host(host.as_ref(), hostname.as_deref(), port, false).await
                    }
                    result => result,
                }
            };
            let result = match self.config.get_connect_timeout() {
                Some(timeout) => compio::time::timeout(*timeout, connect)
                    .await
                    .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))
                    .and_then(|result| result),
                None => connect.await,
            };
            match result {
                Ok(connection) => {
                    let expected = match self.config.get_target_session_attrs() {
                        TargetSessionAttrs::ReadWrite => Some("off"),
                        TargetSessionAttrs::ReadOnly => Some("on"),
                        _ => None,
                    };
                    if let Some(expected) = expected {
                        let row = connection
                            .client
                            .query_one("SHOW transaction_read_only", &[])
                            .await
                            .map_err(io::Error::other)?;
                        let actual: &str = row.try_get(0).map_err(io::Error::other)?;
                        if actual != expected {
                            last_error = io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "PostgreSQL target_session_attrs mismatch",
                            );
                            continue;
                        }
                    }
                    return Ok(connection);
                }
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    async fn connect_host(
        &self,
        host: Option<&Host>,
        hostname: Option<&str>,
        port: u16,
        plaintext: bool,
    ) -> io::Result<Connection> {
        match host {
            Some(Host::Tcp(host)) => {
                let stream = TcpStream::connect((host.as_str(), port)).await?;
                stream.set_nodelay(true)?;
                self.connect_stream(stream, hostname, plaintext).await
            }
            #[cfg(unix)]
            Some(Host::Unix(path)) => {
                let stream =
                    compio::net::UnixStream::connect(path.join(format!(".s.PGSQL.{port}"))).await?;
                self.connect_stream(stream, None, true).await
            }
            None => Err(io::Error::new(io::ErrorKind::InvalidInput, "PostgreSQL host is missing")),
        }
    }

    async fn connect_stream<S>(
        &self,
        stream: S,
        hostname: Option<&str>,
        plaintext: bool,
    ) -> io::Result<Connection>
    where
        S: Splittable + 'static,
        S::ReadHalf: AsyncRead + Unpin,
        S::WriteHalf: AsyncWrite + Unpin,
    {
        let stream = Box::pin(AsyncStream::new(stream)).compat();
        if plaintext || self.config.get_ssl_mode() == SslMode::Disable {
            let mut config = self.config.clone();
            config.ssl_mode(SslMode::Disable);
            let (client, driver) = config
                .connect_raw(stream, tokio_postgres::NoTls)
                .await
                .map_err(io::Error::other)?;
            return Ok(spawn_driver(client, driver));
        }
        #[cfg(any(feature = "native-tls", feature = "rustls"))]
        {
            let domain = hostname.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "PostgreSQL TLS requires a host name alongside hostaddr",
                )
            })?;
            let connector =
                TlsConnector { connector: self.tls.connector().await?, domain: domain.to_owned() };
            let (client, driver) =
                self.config.connect_raw(stream, connector).await.map_err(io::Error::other)?;
            Ok(spawn_driver(client, driver))
        }
        #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
        {
            let _ = hostname;
            let (client, driver) = self
                .config
                .connect_raw(stream, tokio_postgres::NoTls)
                .await
                .map_err(io::Error::other)?;
            Ok(spawn_driver(client, driver))
        }
    }
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("config", &self.config)
            .field("idle", &self.idle.borrow().len())
            .finish_non_exhaustive()
    }
}

impl Lease<'_> {
    /// Borrows the protocol client while this lease owns its connection driver.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.connection.client
    }

    /// Prepares a statement once per session, retaining a bounded cache.
    ///
    /// # Errors
    /// Returns the protocol error if the statement cannot be prepared.
    pub async fn prepare_cached(&self, query: &str) -> Result<Statement, Error> {
        let cached = self
            .connection
            .statements
            .borrow()
            .iter()
            .find(|(sql, _)| sql == query)
            .map(|(_, statement)| statement.clone());
        if let Some(statement) = cached {
            return Ok(statement);
        }
        let statement = self.connection.client.prepare(query).await?;
        let capacity = self.pool.statement_cache_capacity;
        if capacity != 0 {
            let mut statements = self.connection.statements.borrow_mut();
            if statements.len() >= capacity {
                statements.pop_front();
            }
            statements.push_back((query.to_owned(), statement.clone()));
        }
        Ok(statement)
    }

    /// Returns this session after the complete query result has been consumed.
    pub fn recycle(self) {
        self.pool.idle.borrow_mut().push(self.connection);
    }
}

fn spawn_driver<S, T>(client: Client, driver: tokio_postgres::Connection<S, T>) -> Connection
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
    T: tokio_postgres::tls::TlsStream + Unpin + 'static,
{
    let driver = compio::runtime::spawn(async move {
        if let Err(error) = driver.await {
            log::error!("PostgreSQL connection failed: {error}");
        }
    });
    Connection { client, _driver: driver, statements: RefCell::new(VecDeque::new()) }
}

#[cfg(any(feature = "native-tls", feature = "rustls"))]
struct TlsConnector {
    connector: tls::Connector,
    domain: String,
}

#[cfg(any(feature = "native-tls", feature = "rustls"))]
enum TlsStream<S>
where
    S: Splittable,
{
    #[cfg(feature = "native-tls")]
    Native(
        tokio_native_tls::TlsStream<tokio_util::compat::Compat<std::pin::Pin<Box<AsyncStream<S>>>>>,
    ),
    #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
    Rustls(
        tokio_rustls::client::TlsStream<
            tokio_util::compat::Compat<std::pin::Pin<Box<AsyncStream<S>>>>,
        >,
    ),
}

#[cfg(any(feature = "native-tls", feature = "rustls"))]
impl<S>
    tokio_postgres::tls::TlsConnect<tokio_util::compat::Compat<std::pin::Pin<Box<AsyncStream<S>>>>>
    for TlsConnector
where
    S: Splittable + 'static,
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    type Stream = TlsStream<S>;
    type Error = io::Error;
    type Future = futures::future::LocalBoxFuture<'static, io::Result<Self::Stream>>;

    fn connect(
        self,
        stream: tokio_util::compat::Compat<std::pin::Pin<Box<AsyncStream<S>>>>,
    ) -> Self::Future {
        Box::pin(async move {
            match self.connector {
                #[cfg(feature = "native-tls")]
                tls::Connector::Native(connector) => connector
                    .connect(&self.domain, stream)
                    .await
                    .map(TlsStream::Native)
                    .map_err(io::Error::other),
                #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
                tls::Connector::Rustls(connector) => {
                    let domain = self.domain.try_into().map_err(io::Error::other)?;
                    connector.connect(domain, stream).await.map(TlsStream::Rustls)
                }
            }
        })
    }
}

#[cfg(any(feature = "native-tls", feature = "rustls"))]
impl<S> tokio::io::AsyncRead for TlsStream<S>
where
    S: Splittable + 'static,
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(feature = "native-tls")]
            Self::Native(stream) => {
                tokio::io::AsyncRead::poll_read(std::pin::Pin::new(stream), cx, buf)
            }
            #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
            Self::Rustls(stream) => {
                tokio::io::AsyncRead::poll_read(std::pin::Pin::new(stream), cx, buf)
            }
        }
    }
}

#[cfg(any(feature = "native-tls", feature = "rustls"))]
impl<S> tokio::io::AsyncWrite for TlsStream<S>
where
    S: Splittable + 'static,
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(feature = "native-tls")]
            Self::Native(stream) => {
                tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(stream), cx, buf)
            }
            #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
            Self::Rustls(stream) => {
                tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(stream), cx, buf)
            }
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(feature = "native-tls")]
            Self::Native(stream) => {
                tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(stream), cx)
            }
            #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
            Self::Rustls(stream) => {
                tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(stream), cx)
            }
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(feature = "native-tls")]
            Self::Native(stream) => {
                tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(stream), cx)
            }
            #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
            Self::Rustls(stream) => {
                tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(stream), cx)
            }
        }
    }
}

#[cfg(any(feature = "native-tls", feature = "rustls"))]
impl<S> tokio_postgres::tls::TlsStream for TlsStream<S>
where
    S: Splittable + 'static,
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> tokio_postgres::tls::ChannelBinding {
        tokio_postgres::tls::ChannelBinding::none()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn database_url() -> String {
        std::env::var("COMPIO_POSTGRES_TEST_URL").expect("set a test-owned local PostgreSQL URL")
    }

    #[compio::test]
    async fn acquisition_timeout_covers_stalled_handshake() {
        let listener = compio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let url = format!(
            "postgres://user:password@{}/database?sslmode=disable",
            listener.local_addr().unwrap()
        );
        let server = compio::runtime::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            futures::future::pending::<()>().await;
        });
        let pool = Pool::new(&url, NonZeroU32::MIN).unwrap();
        let error = pool.get_with_timeout(Duration::from_millis(20)).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            pool.permits.try_acquire().is_some(),
            "timed-out setup must release its pool permit"
        );
        server.cancel().await;
    }

    #[compio::test]
    #[ignore = "requires a test-owned local PostgreSQL service"]
    async fn pool_limit_and_cancelled_session() {
        let pool = Pool::new(&database_url(), NonZeroU32::MIN).unwrap();
        let session = pool.get().await.unwrap();
        let original: i32 =
            session.client().query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
        let error = pool.get_with_timeout(Duration::from_millis(20)).await.unwrap_err();
        assert_eq!(
            error.kind(),
            io::ErrorKind::TimedOut,
            "a checked-out session must hold the sole pool permit"
        );
        session.recycle();
        let session = pool.get().await.unwrap();
        let reused: i32 =
            session.client().query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
        assert_eq!(reused, original, "a completed session should be reusable");
        assert!(
            compio::time::timeout(
                Duration::from_millis(20),
                session.client().query_one("SELECT pg_sleep(10)", &[])
            )
            .await
            .is_err()
        );
        drop(session);
        let session = pool.get().await.unwrap();
        let replacement: i32 =
            session.client().query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
        assert_ne!(replacement, original, "an unfinished lease must dispose of its session");
    }

    #[compio::test]
    #[ignore = "requires a test-owned local PostgreSQL service"]
    async fn binary_payload_and_integer_parameters() {
        let pool = Pool::new(&database_url(), NonZeroU32::MIN).unwrap();
        let session = pool.get().await.unwrap();
        let chat_id = -100_123_456_789_i64;
        let bytes = vec![0_u8, 255, 13, 10, 42];
        let query = "SELECT $1::BIGINT, $2::BYTEA";
        let statement = session.prepare_cached(query).await.unwrap();
        let cached = session.prepare_cached(query).await.unwrap();
        let row = session.client().query_one(&statement, &[&chat_id, &bytes]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), chat_id);
        assert_eq!(row.get::<_, Vec<u8>>(1), bytes);
        let prepared = session
            .client()
            .query_one(
                "SELECT count(*) FROM pg_prepared_statements WHERE statement = $1",
                &[&query],
            )
            .await
            .unwrap();
        assert_eq!(
            prepared.get::<_, i64>(0),
            1,
            "repeated cache lookup must retain one prepared statement"
        );
        drop(cached);
        session.recycle();
    }

    #[compio::test]
    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    #[ignore = "requires a test-owned PostgreSQL TLS service and root certificate"]
    async fn tls_verification_policies() {
        let root = std::env::var("COMPIO_POSTGRES_TEST_ROOT_CERT")
            .expect("set test-owned root certificate");
        let mut url = url::Url::parse(&database_url()).unwrap();
        url.set_query(None);
        url.set_host(Some("localhost")).unwrap();
        url.query_pairs_mut()
            .append_pair("sslmode", "verify-full")
            .append_pair("sslrootcert", &root);
        let pool = Pool::new(url.as_str(), NonZeroU32::MIN).unwrap();
        let session = pool.get().await.unwrap();
        session.client().query_one("SELECT 1", &[]).await.unwrap();
        drop(session);

        url.set_host(Some("127.0.0.1")).unwrap();
        let pool = Pool::new(url.as_str(), NonZeroU32::MIN).unwrap();
        assert!(
            pool.get().await.is_err(),
            "verify-full must reject a hostname absent from the certificate"
        );

        url.set_query(None);
        url.query_pairs_mut().append_pair("sslmode", "verify-ca").append_pair("sslrootcert", &root);
        let pool = Pool::new(url.as_str(), NonZeroU32::MIN).unwrap();
        pool.get().await.unwrap();

        url.set_query(Some("sslmode=require"));
        let pool = Pool::new(url.as_str(), NonZeroU32::MIN).unwrap();
        pool.get().await.unwrap();

        url.set_query(Some("sslmode=verify-full"));
        let pool = Pool::new(url.as_str(), NonZeroU32::MIN).unwrap();
        assert!(pool.get().await.is_err(), "verify-full must reject an untrusted certificate");

        for (mode, encrypted) in
            [("allow", false), ("disable", false), ("prefer", true), ("require", true)]
        {
            url.set_query(None);
            url.query_pairs_mut().append_pair("sslmode", mode);
            let pool = Pool::new(url.as_str(), NonZeroU32::MIN).unwrap();
            let session = pool.get().await.unwrap();
            let row = session
                .client()
                .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", &[])
                .await
                .unwrap();
            assert_eq!(row.get::<_, bool>(0), encrypted, "TLS negotiation mode {mode}");
        }
    }

    #[compio::test]
    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    #[ignore = "requires a test-owned PostgreSQL database that rejects plaintext connections"]
    async fn allow_retries_with_tls_after_plaintext_rejection() {
        let url = std::env::var("COMPIO_POSTGRES_TLS_ONLY_URL")
            .expect("set the test-owned TLS-only database URL");
        let mut url = url::Url::parse(&url).unwrap();
        url.query_pairs_mut().append_pair("sslmode", "allow");
        let pool = Pool::new(url.as_str(), NonZeroU32::MIN).unwrap();
        let session = pool.get().await.unwrap();
        let row = session
            .client()
            .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", &[])
            .await
            .unwrap();
        assert!(
            row.get::<_, bool>(0),
            "allow must retry using TLS after the server rejects plaintext"
        );
    }
}
