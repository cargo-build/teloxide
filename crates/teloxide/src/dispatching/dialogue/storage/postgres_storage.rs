use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::future::LocalBoxFuture;
use serde::{de::DeserializeOwned, Serialize};
use teloxide_core::types::ChatId;
use thiserror::Error;

use super::{serializer::Serializer, Storage};

/// An error returned from [`PostgresStorage`].
#[derive(Debug, Error)]
pub enum PostgresStorageError<SE>
where
    SE: Debug + Display,
{
    #[error("dialogue serialization error: {0}")]
    SerdeError(SE),

    #[error("postgres error: {0}")]
    PostgresError(#[from] compio_postgres::Error),

    #[error("postgres connection error: {0}")]
    ConnectionError(#[from] std::io::Error),

    #[error("row not found")]
    DialogueNotFound,
}

/// A persistent dialogue storage based on [PostgreSQL](https://www.postgresql.org/).
pub struct PostgresStorage<S> {
    pool: compio_postgres::Pool,
    serializer: S,
}

impl<S> PostgresStorage<S> {
    /// Opens at most `max_connections` sessions and bootstraps the dialogue
    /// table.
    ///
    /// The URL supplies authentication, database, host, port and TLS settings.
    pub async fn open(
        database_url: &str,
        max_connections: u32,
        serializer: S,
    ) -> Result<Arc<Self>, PostgresStorageError<Infallible>> {
        let max_connections = std::num::NonZeroU32::new(max_connections).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_connections must be positive",
            )
        })?;
        let pool = compio_postgres::Pool::new(database_url, max_connections)?;
        let connection = pool.get().await?;
        connection
            .client()
            .batch_execute(include_str!("postgres_storage/queries/create_teloxide_dialogues.sql"))
            .await?;
        connection.recycle();
        Ok(Arc::new(Self { pool, serializer }))
    }
}

impl<S, D> Storage<D> for PostgresStorage<S>
where
    S: Serializer<D> + 'static,
    D: Serialize + DeserializeOwned + 'static,
    <S as Serializer<D>>::Error: Debug + Display,
{
    type Error = PostgresStorageError<<S as Serializer<D>>::Error>;

    fn remove_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> LocalBoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async move {
            let connection = self.pool.get().await?;
            let statement = connection
                .prepare_cached(include_str!("postgres_storage/queries/remove_dialogue.sql"))
                .await?;
            let deleted = connection.client().execute(&statement, &[&chat_id]).await?;
            connection.recycle();
            match deleted {
                0 => Err(PostgresStorageError::DialogueNotFound),
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
                self.serializer.serialize(&dialogue).map_err(PostgresStorageError::SerdeError)?;
            let connection = self.pool.get().await?;
            let statement = connection
                .prepare_cached(include_str!("postgres_storage/queries/update_dialogue.sql"))
                .await?;
            connection.client().execute(&statement, &[&chat_id, &bytes]).await?;
            connection.recycle();
            Ok(())
        })
    }

    fn get_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> LocalBoxFuture<'static, Result<Option<D>, Self::Error>> {
        Box::pin(async move {
            let connection = self.pool.get().await?;
            let statement = connection
                .prepare_cached(include_str!("postgres_storage/queries/get_dialogue.sql"))
                .await?;
            let row = connection.client().query_opt(&statement, &[&chat_id]).await?;
            let bytes: Option<Vec<u8>> = row.map(|row| row.try_get(0)).transpose()?;
            connection.recycle();
            bytes
                .map(|bytes| {
                    self.serializer.deserialize(&bytes).map_err(PostgresStorageError::SerdeError)
                })
                .transpose()
        })
    }
}
