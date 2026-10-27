use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use teloxide::{
    dispatching::dialogue::{PostgresStorage, PostgresStorageError, Serializer, Storage},
    types::ChatId,
};

// These examples are meant to run under the CI with the postgres service
// Were checked locally
#[compio::test]
#[cfg_attr(not(CI_POSTGRES), ignore)]
async fn test_postgres_json() {
    let storage = PostgresStorage::open(
        &database_url("test_postgres_json"),
        1,
        teloxide::dispatching::dialogue::serializer::Json,
    )
    .await
    .unwrap();

    test_postgres(storage).await;
}

#[compio::test]
#[cfg_attr(not(CI_POSTGRES), ignore)]
async fn test_postgres_bincode() {
    let storage = PostgresStorage::open(
        &database_url("test_postgres_bincode"),
        1,
        teloxide::dispatching::dialogue::serializer::Bincode,
    )
    .await
    .unwrap();

    test_postgres(storage).await;
}

#[compio::test]
#[cfg_attr(not(CI_POSTGRES), ignore)]
async fn test_postgres_cbor() {
    let storage = PostgresStorage::open(
        &database_url("test_postgres_cbor"),
        1,
        teloxide::dispatching::dialogue::serializer::Cbor,
    )
    .await
    .unwrap();

    test_postgres(storage).await;
}

type Dialogue = String;

fn database_url(database: &str) -> String {
    let base = match std::env::var("TELOXIDE_TEST_POSTGRES_URL") {
        Ok(base) => base,
        Err(std::env::VarError::NotPresent) => {
            "postgres://teloxide:rewrite_it_in_rust@localhost:5432".to_owned()
        }
        Err(error) => panic!("invalid PostgreSQL test URL environment: {error}"),
    };
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(database);
    url.into()
}

macro_rules! test_dialogues {
    ($storage:expr, $_0:expr, $_1:expr, $_2:expr) => {
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(1)).await.unwrap(), $_0);
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(11)).await.unwrap(), $_1);
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(256)).await.unwrap(), $_2);
    };
}

async fn test_postgres<S>(storage: Arc<PostgresStorage<S>>)
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

    Arc::clone(&storage).update_dialogue(ChatId(1), "replacement".to_owned()).await.unwrap();
    assert_eq!(
        Arc::clone(&storage).get_dialogue(ChatId(1)).await.unwrap(),
        Some("replacement".to_owned())
    );

    Arc::clone(&storage).remove_dialogue(ChatId(1)).await.unwrap();
    Arc::clone(&storage).remove_dialogue(ChatId(11)).await.unwrap();
    Arc::clone(&storage).remove_dialogue(ChatId(256)).await.unwrap();

    test_dialogues!(storage, None, None, None);

    // Check that a try to remove a non-existing dialogue results in an error.
    assert!(matches!(
        Arc::clone(&storage).remove_dialogue(ChatId(1)).await.unwrap_err(),
        PostgresStorageError::DialogueNotFound
    ));
}
