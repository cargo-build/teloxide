use std::{
    io::Cursor,
    sync::{Arc, Mutex},
    time::Duration,
};

use compio::{
    fs::File,
    io::{AsyncReadAtExt, AsyncWriteExt},
};
use futures::TryStreamExt;
use tako::{
    extractors::body::collect_body, router::Router, types::Request, CompioServer, Method,
    StatusCode,
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
    let mut router = Router::new();
    router.route(Method::POST, "/bot123:local/GetMe", |mut request: Request| async move {
        let body = collect_body(&mut request).await.unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap(), serde_json::json!({}));
        r#"{"ok":true,"result":{"id":42,"is_bot":true,"first_name":"Local","username":"local_bot"}}"#
    });
    router.route(Method::POST, "/bot123:local/SetWebhook", move |mut request: Request| {
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
    router.post("/bot123:local/GetUpdates", move |_: Request| {
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
        compio::time::sleep(Duration::from_millis(30)).await;
        bytes::Bytes::from_static(b"slow download")
    });
    router.post("/bot123:local/GetMyDescription", || async {
        compio::time::sleep(Duration::from_millis(30)).await;
        r#"{"ok":true,"result":{"description":"slow reply"}}"#
    });
    let socket = compio::net::TcpListener::bind(([127, 0, 0, 1], 0)).await.unwrap();
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
    server.shutdown(Duration::from_secs(1)).await;
}

fn tokio_runtime_is_absent() -> bool {
    // Tako links Tokio for shared traits; no Tokio executor may be entered.
    tokio::runtime::Handle::try_current().is_err()
}
