use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use futures::future::LocalBoxFuture;
use rusqlite::{Connection, OptionalExtension};
use serde::{de::DeserializeOwned, Serialize};
use teloxide_core::types::ChatId;
use thiserror::Error;

use super::{serializer::Serializer, Storage};

/// A persistent dialogue storage based on [SQLite](https://www.sqlite.org/).
pub struct SqliteStorage<S> {
    connection: Arc<Mutex<Connection>>,
    serializer: S,
}

/// An error returned from [`SqliteStorage`].
#[derive(Debug, Error)]
pub enum SqliteStorageError<SE>
where
    SE: Debug + Display,
{
    #[error("dialogue serialization error: {0}")]
    SerdeError(SE),

    #[error("sqlite error: {0}")]
    SqliteError(#[from] rusqlite::Error),

    #[error("sqlite blocking operation failed: {0}")]
    BlockingError(#[from] std::io::Error),

    #[error("sqlite connection mutex was poisoned")]
    PoisonedConnection,

    /// Returned from [`SqliteStorage::remove_dialogue`].
    #[error("row not found")]
    DialogueNotFound,
}

impl<S> SqliteStorage<S> {
    /// Opens a database and bootstraps the existing dialogue table if absent.
    pub async fn open(
        path: &str,
        serializer: S,
    ) -> Result<Arc<Self>, SqliteStorageError<Infallible>> {
        let path = path.to_owned();
        let connection = compio::runtime::spawn_blocking(move || {
            let connection = Connection::open(path)?;
            connection.busy_timeout(std::time::Duration::from_secs(5))?;
            connection.execute_batch(
                "CREATE TABLE IF NOT EXISTS teloxide_dialogues (
                    chat_id BIGINT PRIMARY KEY,
                    dialogue BLOB NOT NULL
                );",
            )?;
            Ok::<_, rusqlite::Error>(connection)
        })
        .await
        .map_err(std::io::Error::from)??;

        Ok(Arc::new(Self { connection: Arc::new(Mutex::new(connection)), serializer }))
    }

    async fn run<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    ) -> Result<T, SqliteStorageError<Infallible>>
    where
        T: Send + 'static,
    {
        let connection = self.connection.clone();
        compio::runtime::spawn_blocking(move || {
            let connection =
                connection.lock().map_err(|_| SqliteStorageError::PoisonedConnection)?;
            operation(&connection).map_err(SqliteStorageError::SqliteError)
        })
        .await
        .map_err(std::io::Error::from)?
    }
}

impl<SE> SqliteStorageError<SE>
where
    SE: Debug + Display,
{
    fn from_operation(error: SqliteStorageError<Infallible>) -> Self {
        match error {
            SqliteStorageError::SerdeError(error) => match error {},
            SqliteStorageError::SqliteError(error) => Self::SqliteError(error),
            SqliteStorageError::BlockingError(error) => Self::BlockingError(error),
            SqliteStorageError::PoisonedConnection => Self::PoisonedConnection,
            SqliteStorageError::DialogueNotFound => Self::DialogueNotFound,
        }
    }
}

impl<S, D> Storage<D> for SqliteStorage<S>
where
    S: Serializer<D> + 'static,
    D: Serialize + DeserializeOwned + 'static,
    <S as Serializer<D>>::Error: Debug + Display,
{
    type Error = SqliteStorageError<<S as Serializer<D>>::Error>;

    fn remove_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> LocalBoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async move {
            let deleted = self
                .run(move |connection| {
                    connection
                        .execute("DELETE FROM teloxide_dialogues WHERE chat_id = ?", [chat_id])
                })
                .await
                .map_err(SqliteStorageError::from_operation)?;
            match deleted {
                0 => Err(SqliteStorageError::DialogueNotFound),
                _ => Ok(()),
            }
        })
    }

    fn update_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
        dialogue: D,
    ) -> LocalBoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async move {
            let bytes =
                self.serializer.serialize(&dialogue).map_err(SqliteStorageError::SerdeError)?;
            self.run(move |connection| {
                connection.execute(
                    "INSERT INTO teloxide_dialogues VALUES (?, ?)
                     ON CONFLICT(chat_id) DO UPDATE SET dialogue=excluded.dialogue",
                    rusqlite::params![chat_id, bytes],
                )?;
                Ok(())
            })
            .await
            .map_err(SqliteStorageError::from_operation)
        })
    }

    fn get_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> LocalBoxFuture<'static, Result<Option<D>, Self::Error>> {
        Box::pin(async move {
            let bytes: Option<Vec<u8>> = self
                .run(move |connection| {
                    connection
                        .query_row(
                            "SELECT dialogue FROM teloxide_dialogues WHERE chat_id = ?",
                            [chat_id],
                            |row| row.get(0),
                        )
                        .optional()
                })
                .await
                .map_err(SqliteStorageError::from_operation)?;
            bytes
                .map(|bytes| {
                    self.serializer.deserialize(&bytes).map_err(SqliteStorageError::SerdeError)
                })
                .transpose()
        })
    }
}
