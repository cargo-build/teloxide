use std::path::Path;

use bytes::Bytes;
use futures::{future::LocalBoxFuture, stream::LocalBoxStream, FutureExt, StreamExt};

use compio::{
    fs::File,
    io::{AsyncReadAt, AsyncWrite, AsyncWriteExt},
};

use crate::{
    bot::Bot,
    net::{self, Download},
    DownloadError,
};

impl Download for Bot {
    type Err<'dst> = DownloadError;

    // I would like to unbox this, but my coworkers will kill me if they'll see
    // yet another hand written `Future`. (waffle)
    type Fut<'dst> = LocalBoxFuture<'dst, Result<(), Self::Err<'dst>>>;

    fn download_file<'dst, W>(&self, path: &'dst str, destination: &'dst mut W) -> Self::Fut<'dst>
    where
        W: AsyncWrite + ?Sized,
    {
        let is_localhost = match &self.api_url.host_str() {
            Some(host) => ["localhost", "127.0.0.1", "::1"].contains(host),
            None => false,
        };
        // If path is absolute and api_url contains localhost, it is pretty
        // clear there is a local TBA server with --local option, we can just
        // copy the file
        if is_localhost && Path::new(&path).is_absolute() {
            return copy_file(path, destination).boxed_local();
        }

        net::download_file(
            &self.client,
            url::Url::clone(&*self.api_url),
            &self.token,
            path,
            self.request_timeout,
            destination,
        )
        .boxed_local()
    }

    type StreamErr = DownloadError;

    type Stream = LocalBoxStream<'static, Result<Bytes, Self::StreamErr>>;

    fn download_file_stream(&self, path: &str) -> Self::Stream {
        net::download_file_stream(
            &self.client,
            url::Url::clone(&*self.api_url),
            &self.token,
            path,
            self.request_timeout,
        )
        .boxed_local()
    }
}

async fn copy_file<'o, D>(path: &'o str, dst: &'o mut D) -> Result<(), DownloadError>
where
    D: ?Sized + AsyncWrite,
{
    let src_file = File::open(path).await?;
    let mut offset = 0;
    loop {
        let compio::BufResult(result, buffer) =
            src_file.read_at(Vec::with_capacity(128 * 1024), offset).await;
        let len = result?;
        if len == 0 {
            break;
        }
        offset += len as u64;
        let compio::BufResult(result, _) = dst.write_all(buffer).await;
        result?;
    }
    dst.flush().await?;
    Ok(())
}
