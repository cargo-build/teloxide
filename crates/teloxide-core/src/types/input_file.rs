use bytes::Bytes;
use compio::io::AsyncRead;
use futures::{
    future::{ready, Either},
    stream::{self, LocalBoxStream},
    StreamExt, TryStreamExt,
};
use once_cell::sync::OnceCell;
use cyper::{multipart::Part, Body};
use serde::Serialize;


use std::{
    borrow::Cow, fmt, future::Future, io, mem, path::PathBuf, rc::Rc, sync::Arc,
};

use crate::types::{self, InputSticker};

/// This object represents the contents of a file to be uploaded.
///
/// [The official docs](https://core.telegram.org/bots/api#inputfile).
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[cfg_attr(test, schemars(with = "String"))] // It is just a String in the TBA
pub struct InputFile {
    id: OnceCell<Arc<str>>,
    file_name: Option<Cow<'static, str>>,
    inner: InnerFile,
}

#[derive(Clone)]
enum InnerFile {
    Read(Read),
    File(PathBuf),
    Bytes(bytes::Bytes),
    Url(url::Url),
    FileId(types::FileId),
}

use InnerFile::*;

impl InputFile {
    /// Creates an `InputFile` from an url.
    ///
    /// Notes:
    /// - When sending by URL the target file must have the correct MIME type
    ///   (e.g., `audio/mpeg` for [`SendAudio`], etc.).
    /// - In [`SendDocument`], sending by URL will currently only work for
    ///   `GIF`, `PDF` and `ZIP` files.
    /// - To use [`SendVoice`], the file must have the type audio/ogg and be no
    ///   more than 1MB in size. 1-20MB voice notes will be sent as files.
    /// - Other configurations may work but we can't guarantee that they will.
    ///
    /// [`SendAudio`]: crate::payloads::SendAudio
    /// [`SendDocument`]: crate::payloads::SendDocument
    /// [`SendVoice`]: crate::payloads::SendVoice
    #[must_use]
    pub fn url(url: url::Url) -> Self {
        Self::new(Url(url))
    }

    /// Creates an `InputFile` from a file id.
    ///
    /// File id can be obtained from
    ///
    /// Notes:
    /// - It is not possible to change the file type when resending by file id.
    ///   I.e. a video can't be sent as a photo, a photo can't be sent as a
    ///   document, etc.
    /// - It is not possible to resend thumbnails.
    /// - Resending a photo by file id will send all of its [sizes].
    /// - file id is unique for each individual bot and can't be transferred
    ///   from one bot to another.
    /// - file id uniquely identifies a file, but a file can have different
    ///   valid file_ids even for the same bot.
    ///
    /// [sizes]: crate::types::PhotoSize
    pub fn file_id(file_id: types::FileId) -> Self {
        Self::new(FileId(file_id))
    }

    /// Creates an `InputFile` from a file path.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self::new(File(path.into()))
    }

    /// Creates an `InputFile` from a in-memory bytes.
    pub fn memory(data: impl Into<bytes::Bytes>) -> Self {
        Self::new(Bytes(data.into()))
    }

    /// Set the file name for this file.
    pub fn file_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.file_name = Some(name.into());
        self
    }

    /// Creates an `InputFile` from a in-memory bytes.
    ///
    /// Note: in some cases (e.g. sending the same `InputFile` multiple times)
    /// this may read the whole `impl AsyncRead` into memory.
    pub fn read(it: impl AsyncRead + 'static) -> Self {
        Self::new(Read(Read::new(it)))
    }

    /// Shorthand for `Self { file_name: None, inner, id: default() }`
    /// (private because `InnerFile` is private implementation detail)
    fn new(inner: InnerFile) -> Self {
        Self { file_name: None, inner, id: OnceCell::new() }
    }

    /// Returns id of this file.
    ///
    /// This is used to coordinate with `attach://`.
    pub(crate) fn id(&self) -> &str {
        let random = || Arc::from(&*uuid::Uuid::new_v4().as_simple().encode_lower(&mut [0; 32]));
        self.id.get_or_init(random)
    }

    /// Returns `true` if this file needs an attachment i.e. it's not a file_id
    /// or url that can be serialized without any additional multipart parts.
    pub(crate) fn needs_attach(&self) -> bool {
        !matches!(self.inner, Url(_) | FileId(_))
    }

    /// Takes this file out.
    ///
    /// **Note**: this replaces `self` with a dummy value, this function should
    /// only be used when the file is about to get dropped.
    pub(crate) fn take(&mut self) -> Self {
        mem::replace(self, InputFile::file_id(types::FileId(String::new())))
    }

    /// Returns an attach string for `multipart/form-data` in the form of
    /// `"attach://{id}"` if this file should be uploaded via
    /// `multipart/form-data`, or the value if it may be uploaded in any way (ie
    /// it's an URL or file id).
    fn attach_or_value(&self) -> String {
        match &self.inner {
            Url(url) => url.as_str().to_owned(),
            FileId(file_id) => file_id.clone().to_string(),
            _ => {
                const PREFIX: &str = "attach://";

                let id = self.id();
                let mut s = String::with_capacity(PREFIX.len() + id.len());
                s += PREFIX;
                s += id;

                s
            }
        }
    }

    /// Takes the file name or tries to guess it based on file name in the path
    /// if `File.0`. Returns an empty string if couldn't guess.
    fn take_or_guess_filename(&mut self) -> Cow<'static, str> {
        self.file_name.take().unwrap_or_else(|| match &self.inner {
            File(path_to_file) => match path_to_file.file_name() {
                Some(name) => Cow::Owned(name.to_string_lossy().into_owned()),
                None => Cow::Borrowed(""),
            },
            _ => Cow::Borrowed(""),
        })
    }
}

impl fmt::Debug for InnerFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Read(_) => f.debug_struct("Read").finish_non_exhaustive(),
            File(path) => f.debug_struct("File").field("path", path).finish(),
            Bytes(bytes) if f.alternate() => f.debug_tuple("Memory").field(bytes).finish(),
            Bytes(_) => f.debug_struct("Memory").finish_non_exhaustive(),
            Url(url) => f.debug_tuple("Url").field(url).finish(),
            FileId(file_id) => f.debug_tuple("FileId").field(file_id).finish(),
        }
    }
}

impl Serialize for InputFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.attach_or_value().serialize(serializer)
    }
}

// internal api

impl InputFile {
    pub(crate) fn into_part(mut self) -> Option<impl Future<Output = Part>> {
        let filename = self.take_or_guess_filename();

        match self.inner {
            // Url and FileId are serialized just as strings, they don't need additional parts
            Url(_) | FileId(_) => None,

            File(path_to_file) => {
                let fut = async {
                    let body = match compio::fs::File::open(path_to_file).await {
                        Ok(file) => Body::from(file),
                        Err(err) => Body::stream(stream::iter([Err(cyper::Error::System(err))])),
                    };

                    Part::stream(body).file_name(filename)
                };

                Some(Either::Left(fut))
            }
            Bytes(data) => {
                let stream = Part::stream(data).file_name(filename);
                Some(Either::Right(Either::Left(ready(stream))))
            }
            Read(read) => Some(Either::Right(Either::Right(read.into_part(filename)))),
        }
    }
}

/// Keeps cloned readers replayable while an unshared reader streams directly.
#[derive(Clone)]
struct Read {
    inner: Rc<ReadInner>,
}

struct ReadInner {
    buffer: async_lock::Mutex<ReadBuffer>,
    complete: async_lock::OnceCell<Result<Vec<Bytes>, Arc<io::Error>>>,
}

struct ReadBuffer {
    reader: LocalBoxStream<'static, io::Result<Bytes>>,
    chunks: Vec<Bytes>,
}

impl Read {
    fn new(reader: impl AsyncRead + 'static) -> Self {
        let reader = stream::try_unfold(reader, |mut reader| async move {
            let compio::BufResult(len, buffer) = reader.read(Vec::with_capacity(64 * 1024)).await;
            match len? {
                0 => Ok(None),
                _ => Ok(Some((Bytes::from(buffer), reader))),
            }
        })
        .boxed_local();
        Self {
            inner: Rc::new(ReadInner {
                buffer: async_lock::Mutex::new(ReadBuffer { reader, chunks: Vec::new() }),
                complete: async_lock::OnceCell::new(),
            }),
        }
    }

    async fn into_part(self, filename: Cow<'static, str>) -> Part {
        let body = match Rc::try_unwrap(self.inner) {
            Ok(inner) => {
                match inner.complete.into_inner() {
                    Some(result) => buffered_body(result),
                    None => {
                        let stream = inner.buffer.into_inner().reader.map_err(cyper::Error::System);
                        // Cyper's Hyper body requires Send; InputFile and Bot remain local.
                        Body::stream(send_wrapper::SendWrapper::new(stream))
                    }
                }
            }
            Err(inner) => {
                let result = inner.complete.get_or_init(|| async {
                    let mut buffer = inner.buffer.lock().await;
                    while let Some(chunk) = buffer.reader.next().await {
                        match chunk {
                            Ok(chunk) => buffer.chunks.push(chunk),
                            Err(err) => return Err(Arc::new(err)),
                        }
                    }
                    Ok(mem::take(&mut buffer.chunks))
                }).await;
                buffered_body(result.clone())
            }
        };
        Part::stream(body).file_name(filename)
    }
}

fn buffered_body(result: Result<Vec<Bytes>, Arc<io::Error>>) -> Body {
    match result {
        Ok(chunks) => Body::stream(stream::iter(chunks.into_iter().map(Ok))),
        Err(err) => Body::stream(stream::iter([Err(cyper::Error::System(io::Error::other(err)))])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn every_clone_replays_a_cached_reader_error() {
        let reader = stream::iter([
            Ok(Bytes::from_static(b"prefix")),
            Err(io::Error::other("fixture reader failure")),
        ]).boxed_local();
        let read = Read {
            inner: Rc::new(ReadInner {
                buffer: async_lock::Mutex::new(ReadBuffer { reader, chunks: Vec::new() }),
                complete: async_lock::OnceCell::new(),
            }),
        };
        let clones = [read.clone(), read.clone(), read];
        for read in clones {
            let part = read.into_part(Cow::Borrowed("upload.bin")).await;
            let result = cyper::multipart::Form::new().part("file", part).into_stream()
                .try_collect::<Vec<_>>().await;
            assert!(result.unwrap_err().to_string().contains("fixture reader failure"));
        }
    }

    #[compio::test]
    async fn final_clone_after_cancel_keeps_the_partial_buffer() {
        let (release, wait) = futures::channel::oneshot::channel();
        let reader = stream::iter([Ok(Bytes::from_static(b"cached prefix"))])
            .chain(stream::once(async move {
                wait.await.unwrap();
                Ok(Bytes::from_static(b" and suffix"))
            }))
            .boxed_local();
        let read = Read {
            inner: Rc::new(ReadInner {
                buffer: async_lock::Mutex::new(ReadBuffer { reader, chunks: Vec::new() }),
                complete: async_lock::OnceCell::new(),
            }),
        };
        let remaining = read.clone();
        let mut initializing = Box::pin(read.into_part(Cow::Borrowed("upload.bin")));
        assert!(futures::poll!(initializing.as_mut()).is_pending());
        drop(initializing);
        release.send(()).unwrap();
        let part = remaining.into_part(Cow::Borrowed("upload.bin")).await;
        let body = cyper::multipart::Form::new().part("file", part).into_stream()
            .try_collect::<Vec<_>>().await.unwrap().concat();
        let expected = b"cached prefix and suffix";
        assert!(body.windows(expected.len()).any(|chunk| chunk == expected));
    }

    #[compio::test]
    async fn final_reader_clone_replays_the_buffer() {
        let input = InputFile::read(&b"replayed upload bytes"[..]);
        let remaining = input.clone();
        for input in [input, remaining] {
            let part = input.into_part().unwrap().await;
            let chunks = cyper::multipart::Form::new().part("file", part).into_stream()
                .try_collect::<Vec<_>>().await.unwrap();
            let body = chunks.concat();
            assert!(body.windows(b"replayed upload bytes".len()).any(|chunk| chunk == b"replayed upload bytes"));
        }
    }
}

/// An internal trait that is used in expansion of `impl_payload!` used to work
/// with input-file-like things (`InputFile` itself, `Option<InputFile>`,
/// `InputSticker`)
pub(crate) trait InputFileLike {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile));

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile));
}

impl InputFileLike for InputFile {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        into(self.clone())
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        into(self.take())
    }
}

impl InputFileLike for Option<InputFile> {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        if let Some(this) = self {
            this.copy_into(into)
        }
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        if let Some(this) = self {
            this.move_into(into)
        }
    }
}

impl InputFileLike for InputSticker {
    fn copy_into(&self, into: &mut dyn FnMut(InputFile)) {
        self.sticker.copy_into(into)
    }

    fn move_into(&mut self, into: &mut dyn FnMut(InputFile)) {
        self.sticker.move_into(into)
    }
}
