use std::{convert::Infallible, future::Future, sync::Arc, time::Duration};

use futures::{future::Either, FutureExt, StreamExt};
use tako::{
    extractors::body::collect_body,
    responder::Responder,
    router::Router,
    types::{Request as HttpRequest, Response},
    CompioServer, Method, StatusCode,
};

use crate::{
    requests::{Request, Requester},
    stop::{mk_stop_token, StopFlag, StopToken},
    types::{Update, UpdateKind},
    update_listeners::{webhooks::Options, StatefulListener, UpdateListener},
};

/// Registers the webhook and serves updates on the current compio runtime.
/// Binding or serving failures stop the listener and panic in the server task.
pub async fn tako<R>(
    bot: R,
    options: Options,
) -> Result<impl UpdateListener<Err = Infallible>, R::Err>
where
    R: Requester + 'static,
{
    let address = options.address;
    let (mut listener, stop, router) = tako_to_router(bot, options).await?;
    let token = listener.stop_token();

    compio::runtime::spawn(async move {
        let socket = compio::net::TcpListener::bind(address)
            .await
            .inspect_err(|_| token.stop())
            .expect("Couldn't bind to the webhook address");
        let server = CompioServer::builder().build().spawn_http(socket, router);
        let stopped = {
            match futures::future::select(std::pin::pin!(stop), std::pin::pin!(server.result()))
                .await
            {
                Either::Left(_) => true,
                Either::Right((result, _)) => {
                    token.stop();
                    result.expect("Tako webhook server error");
                    false
                }
            }
        };
        if stopped {
            server.shutdown(Duration::from_secs(30)).await;
        }
    })
    .detach();

    Ok(listener)
}

/// Registers a webhook and returns its listener, deletion-on-stop future and
/// router for composition with an existing Tako compio server.
pub async fn tako_to_router<R>(
    bot: R,
    mut options: Options,
) -> Result<(impl UpdateListener<Err = Infallible>, impl Future<Output = ()>, Router), R::Err>
where
    R: Requester,
{
    super::setup_webhook(&bot, &mut options).await?;
    let (listener, stop, router) = tako_no_setup(options);
    let stop = stop.then(move |()| async move {
        if let Err(error) = bot.delete_webhook().send().await {
            log::error!("Couldn't delete webhook: {error}");
        }
    });
    Ok((listener, stop, router))
}

/// Builds a listener, stop future and Tako router without contacting Telegram.
/// Stopping closes the update queue and drains updates that were already
/// accepted.
pub fn tako_no_setup(
    options: Options,
) -> (impl UpdateListener<Err = Infallible>, impl Future<Output = ()>, Router) {
    let (sender, receiver) = async_channel::unbounded();
    let (token, flag) = mk_stop_token();
    let state = Arc::new(WebhookState { sender, flag: flag.clone(), secret: options.secret_token });
    let mut router = Router::new();
    router.route(Method::POST, &options.path, move |request: HttpRequest| {
        telegram_request(Arc::clone(&state), request)
    });

    let stream_flag = flag.clone();
    let stream = futures::stream::unfold((receiver, stream_flag), |(receiver, flag)| async move {
        let update = if flag.is_stopped() {
            receiver.close();
            receiver.recv().await.ok()
        } else {
            match futures::future::select(std::pin::pin!(receiver.recv()), flag.clone()).await {
                Either::Left((result, _)) => result.ok(),
                Either::Right(_) => {
                    receiver.close();
                    receiver.recv().await.ok()
                }
            }
        };
        update.map(|update| (Ok(update), (receiver, flag)))
    })
    .boxed_local();
    let listener = StatefulListener::new(
        (stream, token),
        super::tuple_first_mut,
        |state: &mut (_, StopToken)| state.1.clone(),
    );
    (listener, flag, router)
}

struct WebhookState {
    sender: async_channel::Sender<Update>,
    flag: StopFlag,
    secret: Option<String>,
}

async fn telegram_request(state: Arc<WebhookState>, mut request: HttpRequest) -> Response {
    let secret = request.headers().get("x-telegram-bot-api-secret-token");
    if secret.is_some_and(|value| super::check_secret(value.as_bytes()).is_err()) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let authorized =
        secret.map(|value| value.as_bytes()) == state.secret.as_deref().map(str::as_bytes);
    let body = match collect_body(&mut request).await {
        Ok(body) => body,
        Err(error) => return error.status().into_response(),
    };
    let input = match std::str::from_utf8(&body) {
        Ok(input) => input,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if state.flag.is_stopped() || state.sender.is_closed() {
        state.sender.close();
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match serde_json::from_str::<Update>(input) {
        Ok(mut update) => {
            if let UpdateKind::Error(value) = &mut update.kind {
                *value = serde_json::from_str(input).expect("an Update was parsed from this JSON");
            }
            if state.sender.try_send(update).is_err() {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
        Err(error) => log::error!("Cannot parse an update. Error: {error:?}; value: {input}"),
    }
    StatusCode::OK.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::UpdateId, update_listeners::AsUpdateStream};

    const UPDATE: &str = r#"{"update_id":7,"poll_answer":{"poll_id":"poll","user":{"id":42,"is_bot":false,"first_name":"Test"},"option_ids":[]}}"#;

    #[compio::test]
    async fn webhook_routes_authenticates_and_drains_on_compio() {
        let options = Options::new(
            ([127, 0, 0, 1], 0).into(),
            "https://example.com/webhook".parse().unwrap(),
        )
        .secret_token("known_secret".to_owned());
        let (mut listener, stop, mut router) = tako_no_setup(options);
        router.get("/health", || async { "ready" });
        let socket = compio::net::TcpListener::bind(([127, 0, 0, 1], 0)).await.unwrap();
        let server = CompioServer::builder().build().spawn_http(socket, router);
        let base = format!("http://{}", server.local_addr().unwrap());
        let client = cyper::Client::builder().no_proxy().build().unwrap();

        assert_eq!(
            client
                .get(format!("{base}/health"))
                .unwrap()
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ready"
        );
        for (secret, body, expected) in [
            (Some("known_secret"), UPDATE, StatusCode::OK),
            (Some("wrong_secret"), UPDATE, StatusCode::UNAUTHORIZED),
            (Some("bad secret"), UPDATE, StatusCode::BAD_REQUEST),
            (None, UPDATE, StatusCode::UNAUTHORIZED),
            (Some("known_secret"), "malformed", StatusCode::OK),
        ] {
            let mut request = client.post(format!("{base}/webhook")).unwrap().body(body);
            if let Some(secret) = secret {
                request = request.header("x-telegram-bot-api-secret-token", secret).unwrap();
            }
            assert_eq!(request.send().await.unwrap().status(), expected);
        }

        listener.stop_token().stop();
        stop.await;
        let mut updates = listener.as_stream();
        assert_eq!(updates.next().await.unwrap().unwrap().id, UpdateId(7));
        assert!(updates.next().await.is_none());
        assert_eq!(
            client
                .post(format!("{base}/webhook"))
                .unwrap()
                .header("x-telegram-bot-api-secret-token", "known_secret")
                .unwrap()
                .body(UPDATE)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE,
        );
        server.shutdown(Duration::from_secs(1)).await;
    }

    #[compio::test]
    async fn webhook_bounds_bodies_and_retains_unknown_update() {
        let (sender, receiver) = async_channel::unbounded();
        let (_, flag) = mk_stop_token();
        let state = Arc::new(WebhookState { sender, flag, secret: None });
        let request = |body: Vec<u8>| HttpRequest::new(tako::body::TakoBody::from(body));
        assert_eq!(
            telegram_request(state.clone(), request(vec![b'x'; 2 * 1024 * 1024 + 1]))
                .await
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            telegram_request(state.clone(), request(vec![0xff])).await.status(),
            StatusCode::BAD_REQUEST
        );
        let raw = br#"{"update_id":8,"unknown_kind":{"future":true}}"#;
        assert_eq!(telegram_request(state, request(raw.to_vec())).await.status(), StatusCode::OK);
        let update = receiver.recv().await.unwrap();
        assert_eq!(update.id, UpdateId(8));
        assert!(
            matches!(update.kind, UpdateKind::Error(value) if value["unknown_kind"]["future"] == true)
        );
    }
}
