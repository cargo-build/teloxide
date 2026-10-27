//! Network-specific API.

pub use self::download::{download_file, download_file_stream, Download};

pub(crate) use self::{
    request::{request_json, request_multipart},
    telegram_response::TelegramResponse,
};

mod download;
mod request;
mod telegram_response;

/// The default Telegram API URL.
pub const TELEGRAM_API_URL: &str = "https://api.telegram.org";

pub(crate) const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(17);

pub(crate) fn request_deadline(
    timeout: std::time::Duration,
    hint: Option<std::time::Duration>,
) -> cyper::Result<std::time::Instant> {
    timeout
        .checked_add(hint.unwrap_or(std::time::Duration::ZERO))
        .and_then(|timeout| std::time::Instant::now().checked_add(timeout))
        .ok_or_else(|| {
            cyper::Error::System(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "request deadline exceeds the clock's range",
            ))
        })
}

/// Constructs a network client from the `TELOXIDE_PROXY` environmental
/// variable.
///
/// This function passes the value of `TELOXIDE_PROXY` into
/// [`cyper::proxy::Proxy::all`], if it exists, otherwise returns the default
/// client.
///
/// ## Note
///
/// The created client will have safe settings, meaning that it will be able to
/// work in long time durations, see the [issue 223].
///
/// [`cyper::proxy::Proxy::all`]: https://docs.rs/cyper/latest/cyper/proxy/struct.Proxy.html#method.all
/// [issue 223]: https://github.com/teloxide/teloxide/issues/223
///
/// ## Panics
///
/// If `TELOXIDE_PROXY` exists, but isn't correct url.
#[must_use]
pub fn client_from_env() -> cyper::Client {
    use cyper::proxy::Proxy;

    const TELOXIDE_PROXY: &str = "TELOXIDE_PROXY";

    let builder = default_cyper_settings();

    match std::env::var(TELOXIDE_PROXY).ok() {
        Some(proxy) => {
            builder.proxy(Proxy::all(proxy).expect("cyper::proxy::Proxy creation failed"))
        }
        None => builder,
    }
    .build()
    .expect("creating cyper::Client")
}

/// Returns a native compio HTTP client builder. Configure framework deadlines
/// with [`crate::Bot::set_request_timeout`]; long polls add their payload
/// timeout.
pub fn default_cyper_settings() -> cyper::ClientBuilder {
    cyper::Client::builder()
}

/// Creates URL for making HTTPS requests. See the [Telegram documentation].
///
/// [Telegram documentation]: https://core.telegram.org/bots/api#making-requests
fn method_url(base: url::Url, token: &str, method_name: &str) -> url::Url {
    let mut url = base;
    {
        let mut segments = url.path_segments_mut().expect("base URL cannot be a cannot-be-a-base");
        segments.push(&format!("bot{token}"));
        segments.push(method_name);
    }
    url
}

/// Creates URL for downloading a file. See the [Telegram documentation].
///
/// [Telegram documentation]: https://core.telegram.org/bots/api#file
fn file_url(base: url::Url, token: &str, file_path: &str) -> url::Url {
    let mut url = base;
    {
        let mut segments = url.path_segments_mut().expect("base URL cannot be a cannot-be-a-base");
        segments.push("file");
        segments.push(&format!("bot{token}"));
        segments.push(file_path);
    }
    url
}

#[cfg(test)]
mod tests {
    use crate::net::*;

    #[test]
    fn method_url_test() {
        let url = method_url(
            url::Url::parse(TELEGRAM_API_URL).unwrap(),
            "535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao",
            "methodName",
        );

        assert_eq!(
            url.as_str(),
            "https://api.telegram.org/bot535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao/methodName"
        );
    }

    #[test]
    fn method_url_with_custom_url_test() {
        let url = method_url(
            url::Url::parse("https://example.com/telegram").unwrap(),
            "535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao",
            "methodName",
        );

        assert_eq!(
	    url.as_str(),
	    "https://example.com/telegram/bot535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao/methodName"
	);
    }

    #[test]
    fn file_url_test() {
        let url = file_url(
            url::Url::parse(TELEGRAM_API_URL).unwrap(),
            "535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao",
            "AgADAgADyqoxG2g8aEsu_KjjVsGF4-zetw8ABAEAAwIAA20AA_8QAwABFgQ",
        );

        assert_eq!(
            url.as_str(),
            "https://api.telegram.org/file/bot535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao/AgADAgADyqoxG2g8aEsu_KjjVsGF4-zetw8ABAEAAwIAA20AA_8QAwABFgQ"
        );
    }

    #[test]
    fn file_url_with_custom_url_test() {
        let url = file_url(
            url::Url::parse("https://example.com/telegram").unwrap(),
            "535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao",
            "AgADAgADyqoxG2g8aEsu_KjjVsGF4-zetw8ABAEAAwIAA20AA_8QAwABFgQ",
        );

        assert_eq!(
	    url.as_str(),
	    "https://example.com/telegram/file/bot535362388:AAF7-g0gYncWnm5IyfZlpPRqRRv6kNAGlao/AgADAgADyqoxG2g8aEsu_KjjVsGF4-zetw8ABAEAAwIAA20AA_8QAwABFgQ"
	);
    }
}
