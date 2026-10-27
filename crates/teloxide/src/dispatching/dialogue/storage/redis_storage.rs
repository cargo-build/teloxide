mod connection;

use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::future::LocalBoxFuture;
use redis::IntoConnectionInfo;
use serde::{de::DeserializeOwned, Serialize};
use teloxide_core::types::ChatId;
use thiserror::Error;

use super::{serializer::Serializer, Storage};

/// An error returned from [`RedisStorage`].
#[derive(Debug, Error)]
pub enum RedisStorageError<SE>
where
    SE: Debug + Display,
{
    #[error("parsing/serializing error: {0}")]
    SerdeError(SE),

    #[error("error from Redis: {0}")]
    RedisError(#[from] redis::RedisError),

    /// Returned from [`RedisStorage::remove_dialogue`].
    #[error("row not found")]
    DialogueNotFound,
}

/// A dialogue storage based on [Redis](https://redis.io/).
pub struct RedisStorage<S> {
    pool: connection::Pool,
    serializer: S,
}

impl<S> RedisStorage<S> {
    /// Configures a bounded connection pool using Redis URL authentication and database settings.
    pub async fn open(
        url: &str,
        serializer: S,
    ) -> Result<Arc<Self>, RedisStorageError<Infallible>> {
        let pool = connection::Pool::new(url.into_connection_info()?)?;
        Ok(Arc::new(Self { pool, serializer }))
    }
}

impl<S, D> Storage<D> for RedisStorage<S>
where
    S: Serializer<D> + 'static,
    D: Serialize + DeserializeOwned + 'static,
    <S as Serializer<D>>::Error: Debug + Display,
{
    type Error = RedisStorageError<<S as Serializer<D>>::Error>;

    fn remove_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> LocalBoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async move {
            let deleted: u64 = self.pool.command(redis::cmd("DEL").arg(chat_id)).await?;
            match deleted {
                0 => Err(RedisStorageError::DialogueNotFound),
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
            let dialogue =
                self.serializer.serialize(&dialogue).map_err(RedisStorageError::SerdeError)?;
            self.pool.command::<()>(redis::cmd("SET").arg(chat_id).arg(dialogue)).await?;
            Ok(())
        })
    }

    fn get_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> LocalBoxFuture<'static, Result<Option<D>, Self::Error>> {
        Box::pin(async move {
            let bytes: Option<Vec<u8>> = self.pool.command(redis::cmd("GET").arg(chat_id)).await?;
            bytes
                .map(|bytes| {
                    self.serializer.deserialize(&bytes).map_err(RedisStorageError::SerdeError)
                })
                .transpose()
        })
    }
}
