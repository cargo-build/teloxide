use std::{
    fmt::{Debug, Display},
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use teloxide::{
    dispatching::dialogue::{serializer, Serializer, SqliteStorage, SqliteStorageError, Storage},
    types::ChatId,
};

#[compio::test]
async fn test_sqlite_json() {
    test_sqlite(|| serializer::Json).await;
}

#[compio::test]
async fn test_sqlite_bincode() {
    test_sqlite(|| serializer::Bincode).await;
}

#[compio::test]
async fn test_sqlite_cbor() {
    test_sqlite(|| serializer::Cbor).await;
}

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn new() -> Self {
        static NEXT_DATABASE: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "teloxide-sqlite-{}-{}.sqlite",
            std::process::id(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        );
        Self(std::env::temp_dir().join(name))
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        fs::remove_file(&self.0).expect("remove the test-owned SQLite database");
    }
}

async fn test_sqlite<S>(serializer: impl Fn() -> S)
where
    S: Serializer<String> + 'static,
    <S as Serializer<String>>::Error: Debug + Display,
{
    let database = TestDatabase::new();
    let path = database.0.to_str().unwrap();
    let storage = SqliteStorage::open(path, serializer()).await.unwrap();
    assert_eq!(storage.clone().get_dialogue(ChatId(1)).await.unwrap(), None);
    assert!(matches!(
        storage.clone().remove_dialogue(ChatId(1)).await,
        Err(SqliteStorageError::DialogueNotFound)
    ));

    storage.clone().update_dialogue(ChatId(1), "ABC".to_owned()).await.unwrap();
    storage.clone().update_dialogue(ChatId(11), "DEF".to_owned()).await.unwrap();
    storage.clone().update_dialogue(ChatId(256), "GHI".to_owned()).await.unwrap();
    storage.clone().update_dialogue(ChatId(1), "replacement".to_owned()).await.unwrap();
    assert_eq!(
        storage.clone().get_dialogue(ChatId(1)).await.unwrap(),
        Some("replacement".to_owned())
    );
    drop(storage);

    let persisted = rusqlite::Connection::open(path).unwrap();
    let bytes: Vec<u8> = persisted
        .query_row("SELECT dialogue FROM teloxide_dialogues WHERE chat_id = ?", [1], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        bytes,
        serializer().serialize(&"replacement".to_owned()).unwrap(),
        "persisted bytes must retain the serializer format"
    );
    drop(persisted);

    let reopened = SqliteStorage::open(path, serializer()).await.unwrap();
    assert_eq!(
        reopened.clone().get_dialogue(ChatId(1)).await.unwrap(),
        Some("replacement".to_owned())
    );
    assert_eq!(reopened.clone().get_dialogue(ChatId(11)).await.unwrap(), Some("DEF".to_owned()));
    assert_eq!(reopened.clone().get_dialogue(ChatId(256)).await.unwrap(), Some("GHI".to_owned()));
    for chat_id in [1, 11, 256] {
        Arc::clone(&reopened).remove_dialogue(ChatId(chat_id)).await.unwrap();
        assert_eq!(Arc::clone(&reopened).get_dialogue(ChatId(chat_id)).await.unwrap(), None);
    }
    assert!(matches!(
        reopened.clone().remove_dialogue(ChatId(1)).await,
        Err(SqliteStorageError::DialogueNotFound)
    ));
}
