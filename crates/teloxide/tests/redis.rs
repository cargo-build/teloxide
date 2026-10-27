use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use teloxide::{
    dispatching::dialogue::{RedisStorage, RedisStorageError, Serializer, Storage},
    types::ChatId,
};

#[compio::test]
async fn test_redis_json_protocol_fixture() {
    let (storage, server) =
        protocol_fixture(teloxide::dispatching::dialogue::serializer::Json).await;
    compio::time::timeout(std::time::Duration::from_secs(5), test_redis(storage))
        .await
        .expect("scripted Redis dialogue exchange must finish");
    server.await.unwrap();
}

#[compio::test]
async fn test_redis_bincode_protocol_fixture() {
    let (storage, server) =
        protocol_fixture(teloxide::dispatching::dialogue::serializer::Bincode).await;
    compio::time::timeout(std::time::Duration::from_secs(5), test_redis(storage))
        .await
        .expect("scripted Redis dialogue exchange must finish");
    server.await.unwrap();
}

#[compio::test]
async fn test_redis_cbor_protocol_fixture() {
    let (storage, server) =
        protocol_fixture(teloxide::dispatching::dialogue::serializer::Cbor).await;
    compio::time::timeout(std::time::Duration::from_secs(5), test_redis(storage))
        .await
        .expect("scripted Redis dialogue exchange must finish");
    server.await.unwrap();
}

async fn protocol_fixture<S>(
    serializer: S,
) -> (Arc<RedisStorage<S>>, compio::runtime::JoinHandle<()>)
where
    S: Serializer<Dialogue> + 'static,
    S::Error: Debug,
{
    use compio::{io::compat::AsyncStream, net::TcpListener};
    use futures::io::{AsyncReadExt, AsyncWriteExt};

    let states = [(1, "ABC"), (11, "DEF"), (256, "GHI")];
    let mut script = Vec::new();
    script.extend(
        states
            .iter()
            .map(|(id, _)| (redis::cmd("GET").arg(id).get_packed_command(), b"$-1\r\n".to_vec())),
    );
    script.extend(states.iter().map(|(id, state)| {
        let bytes = serializer.serialize(&(*state).to_owned()).unwrap();
        (redis::cmd("SET").arg(id).arg(bytes).get_packed_command(), b"+OK\r\n".to_vec())
    }));
    script.extend(states.iter().map(|(id, state)| {
        let bytes = serializer.serialize(&(*state).to_owned()).unwrap();
        let response =
            [format!("${}\r\n", bytes.len()).into_bytes(), bytes, b"\r\n".to_vec()].concat();
        (redis::cmd("GET").arg(id).get_packed_command(), response)
    }));
    script.extend(
        states
            .iter()
            .map(|(id, _)| (redis::cmd("DEL").arg(id).get_packed_command(), b":1\r\n".to_vec())),
    );
    script.extend(
        states
            .iter()
            .map(|(id, _)| (redis::cmd("GET").arg(id).get_packed_command(), b"$-1\r\n".to_vec())),
    );
    script.push((redis::cmd("DEL").arg(1).get_packed_command(), b":0\r\n".to_vec()));

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = format!("redis://{}", listener.local_addr().unwrap());
    let server = compio::runtime::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = Box::pin(AsyncStream::new(stream));
        for (expected, response) in script {
            let mut command = vec![0; expected.len()];
            stream.read_exact(&mut command).await.unwrap();
            assert_eq!(
                command, expected,
                "wire command must retain chat-id keys and serialized bytes"
            );
            stream.write_all(&response).await.unwrap();
            stream.flush().await.unwrap();
        }
    });
    let storage = RedisStorage::open(&url, serializer).await.unwrap();
    (storage, server)
}

#[compio::test]
#[cfg_attr(not(CI_REDIS), ignore)]
async fn test_redis_json() {
    let storage = RedisStorage::open(
        "redis://127.0.0.1:7777",
        teloxide::dispatching::dialogue::serializer::Json,
    )
    .await
    .unwrap();
    test_redis(storage).await;
}

#[compio::test]
#[cfg_attr(not(CI_REDIS), ignore)]
async fn test_redis_bincode() {
    let storage = RedisStorage::open(
        "redis://127.0.0.1:7778",
        teloxide::dispatching::dialogue::serializer::Bincode,
    )
    .await
    .unwrap();
    test_redis(storage).await;
}

#[compio::test]
#[cfg_attr(not(CI_REDIS), ignore)]
async fn test_redis_cbor() {
    let storage = RedisStorage::open(
        "redis://127.0.0.1:7779",
        teloxide::dispatching::dialogue::serializer::Cbor,
    )
    .await
    .unwrap();
    test_redis(storage).await;
}

type Dialogue = String;

macro_rules! test_dialogues {
    ($storage:expr, $_0:expr, $_1:expr, $_2:expr) => {
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(1)).await.unwrap(), $_0);
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(11)).await.unwrap(), $_1);
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(256)).await.unwrap(), $_2);
    };
}

async fn test_redis<S>(storage: Arc<RedisStorage<S>>)
where
    S: Serializer<Dialogue> + 'static,
    <S as Serializer<Dialogue>>::Error: Debug + Display,
{
    test_dialogues!(storage, None, None, None);

    Arc::clone(&storage).update_dialogue(ChatId(1), "ABC".to_owned()).await.unwrap();
    Arc::clone(&storage).update_dialogue(ChatId(11), "DEF".to_owned()).await.unwrap();
    Arc::clone(&storage).update_dialogue(ChatId(256), "GHI".to_owned()).await.unwrap();

    test_dialogues!(
        storage,
        Some("ABC".to_owned()),
        Some("DEF".to_owned()),
        Some("GHI".to_owned())
    );

    Arc::clone(&storage).remove_dialogue(ChatId(1)).await.unwrap();
    Arc::clone(&storage).remove_dialogue(ChatId(11)).await.unwrap();
    Arc::clone(&storage).remove_dialogue(ChatId(256)).await.unwrap();

    test_dialogues!(storage, None, None, None);

    // Check that a try to remove a non-existing dialogue results in an error.
    assert!(matches!(
        Arc::clone(&storage).remove_dialogue(ChatId(1)).await.unwrap_err(),
        RedisStorageError::DialogueNotFound
    ));
}
