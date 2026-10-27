use std::{future::Future, sync::Arc, time::Duration};

use cyper::Client;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    net,
    requests::{MultipartPayload, Payload, ResponseResult},
    serde_multipart,
};

mod api;
mod download;

const TELOXIDE_TOKEN: &str = "TELOXIDE_TOKEN";
const TELOXIDE_API_URL: &str = "TELOXIDE_API_URL";

/// A requests sender.
///
/// This is the main type of the library, it allows to send requests to the
/// [Telegram Bot API] and download files.
///
/// ## TBA methods
///
/// All TBA methods are located in the [`Requester`] [`impl for Bot`]. This
/// allows for opt-in behaviours using requester [adaptors].
///
/// ```
/// # async {
/// use teloxide_core::prelude::*;
///
/// let bot = Bot::new("TOKEN");
/// dbg!(bot.get_me().await?);
/// # Ok::<_, teloxide_core::RequestError>(()) };
/// ```
///
/// [`Requester`]: crate::requests::Requester
/// [`impl for Bot`]: Bot#impl-Requester
/// [adaptors]: crate::adaptors
///
/// ## File download
///
/// In the similar way as with TBA methods, file downloading methods are located
/// in a trait — [`Download<'_>`]. See its documentation for more.
///
/// [`Download<'_>`]: crate::net::Download
///
/// ## Clone cost
///
/// `Bot::clone` is relatively cheap, so if you need to share `Bot`, it's
/// recommended to clone it, instead of wrapping it in [`Arc<_>`].
///
/// [`Arc`]: std::sync::Arc
/// [Telegram Bot API]: https://core.telegram.org/bots/api
#[must_use]
#[derive(Debug, Clone)]
pub struct Bot {
    token: Arc<str>,
    api_url: Arc<url::Url>,
    client: Client,
    request_timeout: Duration,
}

/// Constructors
impl Bot {
    /// Creates a new `Bot` with the specified token and the default
    /// [http-client](cyper::Client).
    ///
    /// # Panics
    ///
    /// If it cannot create [`cyper::Client`].
    pub fn new<S>(token: S) -> Self
    where
        S: Into<String>,
    {
        let client = net::default_cyper_settings().build().expect("Client creation failed");

        Self::with_client(token, client)
    }

    /// Creates a new `Bot` with the specified token and your
    /// [`cyper::Client`].
    ///
    /// # Caution
    ///
    /// Your custom client might not be configured correctly to be able to work
    /// in long time durations, see [issue 223].
    ///
    /// [`cyper::Client`]: https://docs.rs/cyper/latest/cyper/struct.Client.html
    /// [issue 223]: https://github.com/teloxide/teloxide/issues/223
    pub fn with_client<S>(token: S, client: Client) -> Self
    where
        S: Into<String>,
    {
        let token = Into::<String>::into(token).into();
        let api_url = Arc::new(
            url::Url::parse(net::TELEGRAM_API_URL)
                .expect("Failed to parse the default TBA URL"),
        );

        Self { token, api_url, client, request_timeout: net::DEFAULT_REQUEST_TIMEOUT }
    }

    /// Creates a new `Bot` with the `TELOXIDE_TOKEN` & `TELOXIDE_API_URL` &
    /// `TELOXIDE_PROXY` environmental variables (the bot's token & the bot's
    /// API URL & the proxy) and the default [`cyper::Client`].
    ///
    /// If `TELOXIDE_API_URL` doesn't exist, returns to the default TBA URL.
    ///
    /// This function passes the value of `TELOXIDE_PROXY` into
    /// [`cyper::proxy::Proxy::all`], if it exists, otherwise returns the default
    /// client.
    ///
    /// # Panics
    ///  - If cannot get the `TELOXIDE_TOKEN`  environmental variable.
    ///  - If `TELOXIDE_API_URL` exists, but isn't a correct URL.
    ///  - If it cannot create [`cyper::Client`].
    ///
    /// [`cyper::Client`]: https://docs.rs/cyper/latest/cyper/struct.Client.html
    /// [`cyper::proxy::Proxy::all`]: https://docs.rs/cyper/latest/cyper/proxy/struct.Proxy.html#method.all
    pub fn from_env() -> Self {
        Self::from_env_with_client(crate::net::client_from_env())
    }

    /// Creates a new `Bot` with the `TELOXIDE_TOKEN` environmental variable
    /// (the bot's token), `TELOXIDE_API_URL` environmental variable (the bot's
    /// API URL) and your [`cyper::Client`].
    ///
    /// If `TELOXIDE_API_URL` doesn't exist, returns to the default TBA URL.
    ///
    /// # Panics
    ///  - If cannot get the `TELOXIDE_TOKEN` environmental variable.
    ///  - If `TELOXIDE_API_URL` exists, but isn't a correct URL.
    ///
    /// # Caution
    /// Your custom client might not be configured correctly to be able to work
    /// in long time durations, see [issue 223].
    ///
    /// [`cyper::Client`]: https://docs.rs/cyper/latest/cyper/struct.Client.html
    /// [issue 223]: https://github.com/teloxide/teloxide/issues/223
    pub fn from_env_with_client(client: Client) -> Self {
        let bot = Self::with_client(get_env(TELOXIDE_TOKEN), client);

        match std::env::var(TELOXIDE_API_URL) {
            Ok(env_api_url) => {
                // Remove the trailing slash if it exists
                let env_api_url = if env_api_url.ends_with('/') {
                    env_api_url.trim_end_matches('/')
                } else {
                    &env_api_url
                };
                let api_url = url::Url::parse(env_api_url)
                    .expect("Failed to parse the `TELOXIDE_API_URL` env variable");
                bot.set_api_url(api_url)
            }
            Err(_) => bot,
        }
    }

    /// Sets a custom API URL.
    ///
    /// For example, you can run your own [TBA server][tbas] and set its URL
    /// using this method.
    ///
    /// [tbas]: https://github.com/tdlib/telegram-bot-api
    ///
    /// ## Examples
    ///
    /// ```
    /// use teloxide_core::{
    ///     requests::{Request, Requester},
    ///     Bot,
    /// };
    ///
    /// # async {
    /// let url = url::Url::parse("https://localhost/tbas").unwrap();
    /// let bot = Bot::new("TOKEN").set_api_url(url);
    /// // From now all methods will use "https://localhost/tbas" as an API URL.
    /// bot.get_me().await
    /// # };
    /// ```
    ///
    /// ## Multi-instance behaviour
    ///
    /// This method only sets the URL for one bot instace, older clones are
    /// unaffected.
    ///
    /// ```
    /// use teloxide_core::Bot;
    ///
    /// let bot = Bot::new("TOKEN");
    /// let bot2 = bot.clone();
    /// let bot = bot.set_api_url(url::Url::parse("https://example.com/").unwrap());
    ///
    /// assert_eq!(bot.api_url().as_str(), "https://example.com/");
    /// assert_eq!(bot.clone().api_url().as_str(), "https://example.com/");
    /// assert_ne!(bot2.api_url().as_str(), "https://example.com/");
    /// ```
    pub fn set_api_url(mut self, url: url::Url) -> Self {
        self.api_url = Arc::new(url);
        self
    }

    /// Sets the request and download deadline; long polls add their payload timeout.
    /// The default is 17 seconds. Existing clones retain their own setting.
    pub fn set_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

/// Getters
impl Bot {
    /// Returns currently used token.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Returns currently used http-client.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns the base request and download deadline.
    #[must_use]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns currently used token API URL.
    #[must_use]
    pub fn api_url(&self) -> url::Url {
        url::Url::clone(&*self.api_url)
    }
}

impl Bot {
    pub(crate) fn execute_json<P>(
        &self,
        payload: &P,
    ) -> impl Future<Output = ResponseResult<P::Output>> + 'static
    where
        P: Payload + Serialize,
        P::Output: DeserializeOwned + 'static,
    {
        let client = self.client.clone();
        let token = Arc::clone(&self.token);
        let api_url = Arc::clone(&self.api_url);

        let request_timeout = self.request_timeout;
        let timeout_hint = payload.timeout_hint();
        let params = stacker::maybe_grow(256 * 1024, 1024 * 1024, || serde_json::to_vec(payload))
            // this `expect` should be ok since we don't write request those may trigger error here
            .expect("serialization of request to be infallible");

        // async move to capture client&token&api_url&params
        async move {
            net::request_json(
                &client,
                token.as_ref(),
                url::Url::clone(&*api_url),
                P::NAME,
                params,
                request_timeout,
                timeout_hint,
            )
            .await
        }
    }

    pub(crate) fn execute_multipart<P>(
        &self,
        payload: &mut P,
    ) -> impl Future<Output = ResponseResult<P::Output>>
    where
        P: MultipartPayload + Serialize,
        P::Output: DeserializeOwned + 'static,
    {
        let client = self.client.clone();
        let token = Arc::clone(&self.token);
        let api_url = Arc::clone(&self.api_url);

        let request_timeout = self.request_timeout;
        let timeout_hint = payload.timeout_hint();
        let params = serde_multipart::to_form(payload);

        // async move to capture client&token&api_url&params
        async move {
            let params = params?.await;
            net::request_multipart(
                &client,
                token.as_ref(),
                url::Url::clone(&*api_url),
                P::NAME,
                params,
                request_timeout,
                timeout_hint,
            )
            .await
        }
    }

    pub(crate) fn execute_multipart_ref<P>(
        &self,
        payload: &P,
    ) -> impl Future<Output = ResponseResult<P::Output>>
    where
        P: MultipartPayload + Serialize,
        P::Output: DeserializeOwned + 'static,
    {
        let client = self.client.clone();
        let token = Arc::clone(&self.token);
        let api_url = self.api_url.clone();

        let request_timeout = self.request_timeout;
        let timeout_hint = payload.timeout_hint();
        let params = serde_multipart::to_form_ref(payload);

        // async move to capture client&token&api_url&params
        async move {
            let params = params?.await;
            net::request_multipart(
                &client,
                token.as_ref(),
                url::Url::clone(&*api_url),
                P::NAME,
                params,
                request_timeout,
                timeout_hint,
            )
            .await
        }
    }
}

fn get_env(env: &'static str) -> String {
    std::env::var(env).unwrap_or_else(|_| panic!("Cannot get the {env} env variable"))
}
