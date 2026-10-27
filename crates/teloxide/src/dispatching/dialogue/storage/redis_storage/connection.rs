use std::{cell::RefCell, collections::VecDeque, io, io::Read, pin::Pin};

use async_lock::Semaphore;
use compio::{io::compat::AsyncStream, net::TcpStream};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use redis::{
    ConnectionAddr, ConnectionInfo, FromRedisValue, Parser, ProtocolVersion, RedisError,
    RedisResult, Value,
};

pub(super) struct Pool {
    info: ConnectionInfo,
    idle: RefCell<VecDeque<Connection>>,
    permits: Semaphore,
}

struct Connection {
    stream: Pin<Box<dyn Stream>>,
    parser: Parser,
    unread: Vec<u8>,
}

trait Stream: AsyncRead + AsyncWrite {}
impl<S> Stream for S where S: AsyncRead + AsyncWrite {}

struct Available<'reader, 'bytes>(&'reader mut &'bytes [u8]);

impl Read for Available<'_, '_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.0.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Read::read(self.0, bytes)
    }
}

impl Pool {
    pub(super) fn new(info: ConnectionInfo) -> RedisResult<Self> {
        if matches!(info.addr, ConnectionAddr::TcpTls { .. }) {
            return Err((redis::ErrorKind::InvalidClientConfig, "Redis TLS is not enabled").into());
        }
        let max_connections = num_cpus::get() * 2;
        Ok(Self {
            info,
            idle: RefCell::new(VecDeque::new()),
            permits: Semaphore::new(max_connections),
        })
    }

    pub(super) async fn command<T>(&self, command: &redis::Cmd) -> RedisResult<T>
    where
        T: FromRedisValue,
    {
        let _permit = self.permits.acquire().await;
        let idle = self.idle.borrow_mut().pop_front();
        let mut connection = match idle {
            Some(connection) => connection,
            None => Connection::connect(&self.info).await?,
        };
        // A cancelled or failed exchange owns and drops its socket before
        // releasing the permit.
        let value = connection.command(command).await?;
        self.idle.borrow_mut().push_back(connection);
        T::from_owned_redis_value(value)
    }
}

impl Connection {
    async fn connect(info: &ConnectionInfo) -> RedisResult<Self> {
        let stream: Pin<Box<dyn Stream>> = match &info.addr {
            ConnectionAddr::Tcp(host, port) => {
                let stream = TcpStream::connect((host.as_str(), *port)).await?;
                stream.set_nodelay(false)?;
                Box::pin(AsyncStream::new(stream))
            }
            #[cfg(unix)]
            ConnectionAddr::Unix(path) => {
                Box::pin(AsyncStream::new(compio::net::UnixStream::connect(path).await?))
            }
            _ => {
                return Err((
                    redis::ErrorKind::InvalidClientConfig,
                    "unsupported Redis connection address",
                )
                    .into())
            }
        };
        let mut connection = Self { stream, parser: Parser::new(), unread: Vec::new() };
        if info.redis.protocol == ProtocolVersion::RESP3 {
            let mut hello = redis::cmd("HELLO");
            hello.arg(3);
            if let Some(password) = &info.redis.password {
                let username = match &info.redis.username {
                    Some(username) => username.as_str(),
                    None => "default",
                };
                hello.arg("AUTH").arg(username).arg(password);
            }
            connection.command(&hello).await?;
        } else if let Some(password) = &info.redis.password {
            let mut auth = redis::cmd("AUTH");
            if let Some(username) = &info.redis.username {
                auth.arg(username);
            }
            auth.arg(password);
            let response = match connection.command(&auth).await {
                Err(error)
                    if info.redis.username.is_some()
                        && error.detail().is_some_and(|detail| {
                            detail.contains("wrong number of arguments for 'auth' command")
                        }) =>
                {
                    connection.command(redis::cmd("AUTH").arg(password)).await?
                }
                result => result?,
            };
            if response != Value::Okay {
                return Err((
                    redis::ErrorKind::AuthenticationFailed,
                    "Redis server refused to authenticate",
                )
                    .into());
            }
        }
        if info.redis.db != 0 {
            connection.command(redis::cmd("SELECT").arg(info.redis.db)).await?;
        }
        Ok(connection)
    }

    async fn command(&mut self, command: &redis::Cmd) -> RedisResult<Value> {
        self.stream.write_all(&command.get_packed_command()).await?;
        self.stream.flush().await?;
        loop {
            let mut available = self.unread.as_slice();
            let result = self.parser.parse_value(Available(&mut available));
            let consumed = self.unread.len() - available.len();
            self.unread.drain(..consumed);
            match result {
                Ok(Value::Push { .. }) => continue,
                Ok(value) => return value.extract_error(),
                Err(error) if error.is_timeout() => (),
                Err(error) => return Err(error),
            }
            let mut buffer = [0; 8192];
            let count = self.stream.read(&mut buffer).await?;
            if count == 0 {
                return Err(RedisError::from(io::Error::from(io::ErrorKind::UnexpectedEof)));
            }
            self.unread.extend_from_slice(&buffer[..count]);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, time::Duration};

    use compio::net::TcpListener;
    use futures::{
        channel::oneshot,
        io::{AsyncReadExt, AsyncWriteExt},
    };
    use redis::IntoConnectionInfo;

    use super::*;

    #[compio::test]
    async fn cancelled_reply_is_never_reused() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let pool = Rc::new(
            Pool::new(format!("redis://{address}").into_connection_info().unwrap()).unwrap(),
        );
        let (received, wait_received) = oneshot::channel();
        let request = redis::cmd("GET").arg(123).get_packed_command();
        let server = compio::runtime::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = Box::pin(AsyncStream::new(first));
            let mut bytes = vec![0; request.len()];
            first.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, request);
            received.send(()).unwrap();
            let (second, _) = listener.accept().await.unwrap();
            let mut second = Box::pin(AsyncStream::new(second));
            second.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, request);
            let _ = first.write_all(b"$5\r\nstale\r\n").await;
            second.write_all(b"$5\r\nfresh\r\n").await.unwrap();
            second.flush().await.unwrap();
        });
        let pending = compio::runtime::spawn({
            let pool = pool.clone();
            async move { pool.command::<Vec<u8>>(redis::cmd("GET").arg(123)).await }
        });
        wait_received.await.unwrap();
        pending.cancel().await;
        assert!(pool.idle.borrow().is_empty(), "cancelled connection must stay out of the pool");
        let fresh: Vec<u8> =
            compio::time::timeout(Duration::from_secs(5), pool.command(redis::cmd("GET").arg(123)))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(fresh, b"fresh");
        server.await.unwrap();
    }

    #[compio::test]
    async fn fragmented_reply_and_authentication_database_handshake() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let pool =
            Pool::new(format!("redis://user:secret@{address}/3").into_connection_info().unwrap())
                .unwrap();
        let server = compio::runtime::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = Box::pin(AsyncStream::new(stream));
            let commands = [
                (
                    redis::cmd("AUTH").arg("user").arg("secret").get_packed_command(),
                    b"+OK\r\n".as_slice(),
                ),
                (redis::cmd("SELECT").arg(3).get_packed_command(), b"+OK\r\n".as_slice()),
                (redis::cmd("GET").arg(123).get_packed_command(), b"$5\r\nhello\r\n".as_slice()),
            ];
            for (command, response) in commands {
                let mut request = vec![0; command.len()];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(request, command);
                for byte in response {
                    stream.write_all(&[*byte]).await.unwrap();
                    stream.flush().await.unwrap();
                    compio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
        let bytes: Vec<u8> =
            compio::time::timeout(Duration::from_secs(5), pool.command(redis::cmd("GET").arg(123)))
                .await
                .expect("fragmented Redis response must finish")
                .unwrap();
        assert_eq!(bytes, b"hello");
        server.await.unwrap();
    }
}
