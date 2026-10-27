#![cfg(feature = "webhooks-tako")]

use std::{sync::{Arc, Mutex}, time::{Duration, Instant}};

use futures::StreamExt;
use tako::{extractors::body::collect_body, router::Router, types::Request, CompioServer};
use teloxide::{prelude::*, types::AllowedUpdate, update_listeners::{AsUpdateStream, Polling, UpdateListener}};

#[compio::test]
async fn polling_backs_off_retains_offsets_and_acknowledges_stop() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let mut router = Router::new();
    router.post("/bot123:local/GetUpdates", move |mut request: Request| {
        let captured = captured.clone();
        async move {
            let body = collect_body(&mut request).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let mut requests = captured.lock().unwrap();
            requests.push(body);
            match requests.len() {
                1 => r#"{"ok":false,"error_code":400,"description":"Bad Request: local fixture"}"#,
                2 => r#"{"ok":true,"result":[{"update_id":7,"future_update":{}},{"update_id":8,"future_update":{}}]}"#,
                3 => r#"{"ok":true,"result":[]}"#,
                _ => panic!("unexpected polling request"),
            }
        }
    });
    let socket = compio::net::TcpListener::bind(([127, 0, 0, 1], 0)).await.unwrap();
    let server = CompioServer::builder().build().spawn_http(socket, router);
    let client = cyper::Client::builder().no_proxy().build().unwrap();
    let bot = Bot::with_client("123:local", client)
        .set_api_url(format!("http://{}", server.local_addr().unwrap()).parse().unwrap());
    let delay = Duration::from_millis(10);
    let mut polling = Polling::builder(bot)
        .timeout(Duration::from_secs(3))
        .limit(2)
        .allowed_updates(vec![AllowedUpdate::Message])
        .backoff_strategy(move |_| delay)
        .build();
    let stop = polling.stop_token();
    let mut updates = std::pin::pin!(polling.as_stream());
    assert!(updates.next().await.unwrap().is_err());
    let retry_started = Instant::now();
    assert_eq!(updates.next().await.unwrap().unwrap().id.0, 7);
    assert!(retry_started.elapsed() >= delay);
    assert_eq!(updates.next().await.unwrap().unwrap().id.0, 8);
    stop.stop();
    assert!(updates.next().await.is_none());
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0]["allowed_updates"], serde_json::json!(["message"]));
    assert_eq!(requests[0]["offset"], 0);
    assert_eq!(requests[1]["offset"], 0);
    assert_eq!(requests[1]["timeout"], 3);
    assert_eq!(requests[2]["offset"], 9);
    assert_eq!(requests[2]["limit"], 1);
    assert_eq!(requests[2]["timeout"], 0);
    drop(requests);
    server.shutdown(Duration::from_secs(1)).await;
}
