use std::{
    io::Cursor,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use compio::{
    fs::File,
    io::{AsyncReadAtExt, AsyncWriteExt},
};
use futures::TryStreamExt;
use tako::{
    extractors::body::collect_body, router::Router, types::Request as HttpRequest, CompioServer,
    Method, StatusCode,
};
use teloxide_core::{net::Download, prelude::*, types::InputFile, DownloadError};

#[compio::test]
async fn bot_json_multipart_and_downloads_use_compio_io() {
    #[cfg(target_os = "linux")]
    assert_eq!(
        compio::runtime::Runtime::current().driver_type(),
        compio::driver::DriverType::IoUring
    );
    assert!(tokio_runtime_is_absent());

    let uploaded = Arc::new(Mutex::new(Vec::new()));
    let capture = uploaded.clone();
    let message_attempts = Arc::new(AtomicUsize::new(0));
    let attempts = message_attempts.clone();
    let mut router = Router::new();
    router.route(Method::POST, "/bot123:local/GetMe", |mut request: HttpRequest| async move {
        let body = collect_body(&mut request).await.unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap(), serde_json::json!({}));
        r#"{"ok":true,"result":{"id":42,"is_bot":true,"first_name":"Local","username":"local_bot","can_join_groups":true,"can_read_all_group_messages":false,"supports_inline_queries":false,"can_connect_to_business":false,"has_main_web_app":false}}"#
    });
    router.route(Method::POST, "/bot123:local/SetWebhook", move |mut request: HttpRequest| {
        let capture = capture.clone();
        async move {
            assert!(request.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("multipart/form-data; boundary="));
            let body = collect_body(&mut request).await.unwrap();
            capture.lock().unwrap().push(body);
            r#"{"ok":true,"result":true}"#
        }
    });
    let (started, wait_started) = async_channel::bounded(1);
    router.post("/bot123:local/GetUpdates", move |_: HttpRequest| {
        let started = started.clone();
        async move {
            started.send(()).await.unwrap();
            futures::future::pending::<&'static str>().await
        }
    });
    router.get("/file/bot123:local/test.bin", || async {
        bytes::Bytes::from_static(b"downloaded bytes")
    });
    router.get("/file/bot123:local/missing.bin", || async { StatusCode::NOT_FOUND });
    router.get("/file/bot123:local/slow.bin", || async {
        server_delay().await;
        bytes::Bytes::from_static(b"slow download")
    });
    router.post("/bot123:local/GetMyDescription", || async {
        server_delay().await;
        r#"{"ok":true,"result":{"description":"slow reply"}}"#
    });
    router.post("/bot123:local/SendMessage", move |mut request: HttpRequest| {
        let attempts = attempts.clone();
        async move {
            let body = collect_body(&mut request).await.unwrap();
            let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(payload["text"], "queued");
            match attempts.fetch_add(1, Ordering::Relaxed) {
                0 => r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 1","parameters":{"retry_after":1}}"#,
                _ => r#"{"ok":true,"result":{"message_id":5,"date":1,"chat":{"id":1,"type":"private"},"text":"queued"}}"#,
            }
        }
    });
    let socket = compio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let server = CompioServer::builder().build().spawn_http(socket, router);
    let client = cyper::Client::builder().no_proxy().build().unwrap();
    let bot = Bot::with_client("123:local", client)
        .set_api_url(format!("http://{}", server.local_addr().unwrap()).parse().unwrap());
    assert_eq!(bot.get_me().await.unwrap().user.id.0, 42);
    assert_eq!(bot.request_timeout(), Duration::from_secs(17));
    let impatient = bot.clone().set_request_timeout(Duration::from_millis(5));
    assert!(matches!(impatient.get_my_description().await,
        Err(teloxide_core::RequestError::Network(error)) if matches!(*error, cyper::Error::Timeout)));
    let patient = bot.clone().set_request_timeout(Duration::from_secs(1));
    assert_eq!(patient.get_my_description().await.unwrap().description, "slow reply");
    assert!(matches!(impatient.download_file("slow.bin", &mut Vec::new()).await,
        Err(DownloadError::Network(error)) if matches!(*error, cyper::Error::Timeout)));
    assert!(matches!(impatient.download_file_stream("slow.bin").try_collect::<Vec<_>>().await,
        Err(DownloadError::Network(error)) if matches!(*error, cyper::Error::Timeout)));
    let mut slow_download = Vec::new();
    patient.download_file("slow.bin", &mut slow_download).await.unwrap();
    assert_eq!(slow_download, b"slow download");
    let propagated: teloxide_core::RequestError =
        DownloadError::HttpStatus(StatusCode::NOT_FOUND).into();
    assert!(matches!(propagated, teloxide_core::RequestError::HttpStatus(StatusCode::NOT_FOUND)));

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("source.bin");
    let mut source = Cursor::new(File::create(&path).await.unwrap());
    source.write_all(b"file upload bytes".to_vec()).await.0.unwrap();
    let input =
        InputFile::read(Cursor::new(File::open(&path).await.unwrap())).file_name("source.bin");
    let replay = input.clone();
    for input in [input, replay, InputFile::file(&path).file_name("source.bin")] {
        bot.set_webhook("https://example.com/webhook".parse().unwrap())
            .certificate(input)
            .await
            .unwrap();
    }
    assert!(uploaded.lock().unwrap().iter().all(|body| {
        let body = std::str::from_utf8(body).unwrap();
        body.contains("file upload bytes")
            && body.contains("filename=\"source.bin\"")
            && body.contains("attach://")
    }));

    let mut downloaded = Vec::new();
    bot.download_file("test.bin", &mut downloaded).await.unwrap();
    assert_eq!(downloaded, b"downloaded bytes");
    let chunks = bot.download_file_stream("test.bin").try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(chunks.concat(), b"downloaded bytes");
    assert!(matches!(
        bot.download_file("missing.bin", &mut Vec::new()).await,
        Err(DownloadError::HttpStatus(StatusCode::NOT_FOUND))
    ));

    let mut copied = Vec::new();
    bot.download_file(path.to_str().unwrap(), &mut copied).await.unwrap();
    assert_eq!(copied, b"file upload bytes");
    let (_, original) =
        File::open(&path).await.unwrap().read_to_end_at(Vec::new(), 0).await.unwrap();
    assert_eq!(copied, original);
    let pending = compio::runtime::spawn({
        let bot = bot.clone();
        async move { bot.get_updates().await }
    });
    wait_started.recv().await.unwrap();
    pending.cancel().await;
    assert_eq!(bot.get_me().await.unwrap().user.id.0, 42);
    #[cfg(feature = "throttle")]
    {
        let throttled = bot.clone().throttle(teloxide_core::adaptors::throttle::Limits::default());
        let message = compio::time::timeout(
            Duration::from_secs(3),
            throttled.send_message(ChatId(1), "queued").send(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(message.id.0, 5);
        assert_eq!(message_attempts.load(Ordering::Relaxed), 2);
    }
    server.shutdown(Duration::from_secs(1)).await;
}

fn tokio_runtime_is_absent() -> bool {
    // Tako links Tokio for shared traits; no Tokio executor may be entered.
    tokio::runtime::Handle::try_current().is_err()
}

async fn server_delay() {
    let (finished, wait) = futures::channel::oneshot::channel();
    // Tako requires a Send handler future; the timer stays in its local compio
    // task.
    compio::runtime::spawn(async move {
        compio::time::sleep(Duration::from_millis(30)).await;
        let _ = finished.send(());
    })
    .detach();
    wait.await.unwrap();
}
