use std::{future::Future, time::{Duration, Instant}};

use bytes::Bytes;
use futures::{Stream, StreamExt, TryFutureExt};
use cyper::Client;
use compio::io::{AsyncWrite, AsyncWriteExt};
use url::Url;

use crate::{errors::DownloadError, net::file_url};

/// A trait for downloading files from Telegram.
pub trait Download {
    /// An error returned from [`download_file`](Self::download_file).
    type Err<'dst>;

    /// A future returned from [`download_file`](Self::download_file).
    type Fut<'dst>: Future<Output = Result<(), Self::Err<'dst>>>;

    // NOTE: We currently only allow borrowing `dst` in the future,
    //       however we could also allow borrowing `self` or `path`.
    //       This doesn't seem useful for our current implementers of
    //       `Download`, but we could.

    /// Download a file from Telegram into `destination`.
    ///
    /// `path` can be obtained from [`GetFile`].
    ///
    /// If the bot uses a [local bot api](https://github.com/tdlib/telegram-bot-api), this function
    /// just copies the file into `destination`.
    ///
    /// To download as a stream of chunks, see [`download_file_stream`].
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use teloxide_core::{
    ///     net::Download,
    ///     requests::{Request, Requester},
    ///     types::{File, FileId},
    ///     Bot,
    /// };
    /// use compio::fs;
    /// use std::io::Cursor;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let bot = Bot::new("TOKEN");
    ///
    /// let file = bot.get_file(FileId("*file_id*".to_string())).await?;
    /// let mut dst = Cursor::new(fs::File::create("/tmp/test.png").await?);
    /// bot.download_file(&file.path, &mut dst).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [`GetFile`]: crate::payloads::GetFile
    /// [`download_file_stream`]: Self::download_file_stream
    fn download_file<'dst, W>(
        &self,
        path: &'dst str,
        destination: &'dst mut W,
    ) -> Self::Fut<'dst>
    where
        W: AsyncWrite + ?Sized;

    /// An error returned from
    /// [`download_file_stream`](Self::download_file_stream).
    type StreamErr;

    /// A stream returned from [`download_file_stream`].
    ///
    ///[`download_file_stream`]: (Self::download_file_stream)
    type Stream: Stream<Item = Result<Bytes, Self::StreamErr>>;

    /// Download a file from Telegram as [`Stream`].
    ///
    /// `path` can be obtained from the [`GetFile`].
    ///
    /// To download into an [`AsyncWrite`] (e.g. [`compio::fs::File`]), see
    /// [`download_file`].
    ///
    /// [`GetFile`]: crate::payloads::GetFile
    /// [`AsyncWrite`]: compio::io::AsyncWrite
    /// [`compio::fs::File`]: compio::fs::File
    /// [`download_file`]: Self::download_file
    fn download_file_stream(&self, path: &str) -> Self::Stream;
}

/// Download a file from Telegram into `dst`.
///
/// Note: if you don't need to use a different (from you're bot) client and
/// don't need to get *all* performance (and you don't, c'mon it's very io-bound
/// job), then it's recommended to use [`Download::download_file`].
pub fn download_file<'o, D>(
    client: &Client,
    api_url: Url,
    token: &str,
    path: &str,
    timeout: Duration,
    dst: &'o mut D,
) -> impl Future<Output = Result<(), DownloadError>> + 'o
where
    D: ?Sized + AsyncWrite,
{
    let stream = download_file_stream(client, api_url, token, path, timeout);
    async move {
        let mut stream = std::pin::pin!(stream);
        while let Some(chunk) = stream.next().await {
            let compio::BufResult(result, _) = dst.write_all(chunk?).await;
            result?;
        }
        Ok(())
    }
}

/// Download a file from Telegram as [`Stream`].
///
/// Note: if you don't need to use a different (from you're bot) client and
/// don't need to get *all* performance (and you don't, c'mon it's very io-bound
/// job), then it's recommended to use [`Download::download_file_stream`].
pub fn download_file_stream(
    client: &Client,
    api_url: Url,
    token: &str,
    path: &str,
    timeout: Duration,
) -> impl Stream<Item = Result<Bytes, DownloadError>> + 'static {
    let request = client.get(file_url(api_url, token, path));
    async move {
        let deadline = super::request_deadline(timeout, None)?;
        let response = compio::time::timeout_at(deadline, request?.send())
            .await.map_err(|_| cyper::Error::Timeout)??;
        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            return Err(DownloadError::HttpStatus(status));
        }
        Ok(deadline_stream(response.bytes_stream(), deadline))
    }
    .try_flatten_stream()
}

fn deadline_stream<S>(stream: S, deadline: Instant) -> impl Stream<Item = Result<Bytes, DownloadError>>
where
    S: Stream<Item = cyper::Result<Bytes>> + 'static,
{
    futures::stream::unfold(Some(Box::pin(stream)), move |stream| async move {
        let mut stream = stream?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let next = if remaining.is_zero() {
            Some(Err(cyper::Error::Timeout))
        } else {
            match compio::time::timeout(remaining, stream.next()).await {
                Ok(next) => next,
                Err(_) => Some(Err(cyper::Error::Timeout)),
            }
        };
        next.map(|item| match item {
            Ok(bytes) => (Ok(bytes), Some(stream)),
            Err(error) => (Err(DownloadError::from(error)), None),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn stalled_body_returns_one_timeout_then_ends() {
        let stream = deadline_stream(futures::stream::pending(), Instant::now() + Duration::from_millis(5));
        let mut stream = std::pin::pin!(stream);
        assert!(matches!(stream.next().await, Some(Err(DownloadError::Network(error))) if matches!(*error, cyper::Error::Timeout)));
        assert!(stream.next().await.is_none());
    }
}
