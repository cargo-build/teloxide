//! Convenient error handling.

use futures::future::LocalBoxFuture;
use std::{convert::Infallible, fmt::Debug, future::Future, rc::Rc};

/// An asynchronous handler of an error.
///
/// See [the module-level documentation for the design
/// overview](crate::dispatching).
pub trait ErrorHandler<E> {
    fn handle_error(self: Rc<Self>, error: E) -> LocalBoxFuture<'static, ()>;
}

impl<E, F, Fut> ErrorHandler<E> for F
where
    F: Fn(E) -> Fut + 'static,
    E: 'static,
    Fut: Future<Output = ()> + 'static,
{
    fn handle_error(self: Rc<Self>, error: E) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move { self(error).await })
    }
}

/// Something that can be handled by an error handler.
///
/// ## Examples
/// ```
/// use teloxide::error_handlers::OnError;
///
/// # #[compio::main]
/// # async fn main_() {
/// // Prints nothing
/// let ok: Result<i32, i32> = Ok(200);
/// ok.log_on_error().await;
///
/// // Prints "Error: 404"
/// let err: Result<i32, i32> = Err(404);
/// err.log_on_error().await;
/// # }
/// ```
///
/// Use an arbitrary error handler:
/// ```
/// use teloxide::error_handlers::{IgnoringErrorHandler, OnError};
///
/// # #[compio::main]
/// # async fn main_() {
/// let err: Result<i32, i32> = Err(404);
/// err.on_error(IgnoringErrorHandler::new()).await;
/// # }
/// ```
pub trait OnError<E> {
    fn on_error<'a, Eh>(self, eh: Rc<Eh>) -> LocalBoxFuture<'a, ()>
    where
        Self: 'a,
        Eh: ErrorHandler<E>,
        Rc<Eh>: 'a;

    /// A shortcut for `.on_error(LoggingErrorHandler::new())`.
    fn log_on_error<'a>(self) -> LocalBoxFuture<'a, ()>
    where
        Self: Sized + 'a,
        E: Debug,
    {
        self.on_error(LoggingErrorHandler::new())
    }
}

impl<T, E> OnError<E> for Result<T, E> {
    fn on_error<'a, Eh>(self, eh: Rc<Eh>) -> LocalBoxFuture<'a, ()>
    where
        Self: 'a,
        Eh: ErrorHandler<E>,
        Rc<Eh>: 'a,
    {
        Box::pin(async move {
            if let Err(error) = self {
                eh.handle_error(error).await;
            }
        })
    }
}

/// A handler that silently ignores all errors.
///
/// ## Example
/// ```
/// # #[compio::main]
/// # async fn main_() {
/// use teloxide::error_handlers::{ErrorHandler, IgnoringErrorHandler};
///
/// IgnoringErrorHandler::new().handle_error(()).await;
/// IgnoringErrorHandler::new().handle_error(404).await;
/// IgnoringErrorHandler::new().handle_error("error").await;
/// # }
/// ```
#[derive(Clone, Copy)]
pub struct IgnoringErrorHandler;

impl IgnoringErrorHandler {
    #[must_use]
    pub fn new() -> Rc<Self> {
        Rc::new(Self)
    }
}

impl<E> ErrorHandler<E> for IgnoringErrorHandler {
    fn handle_error(self: Rc<Self>, _: E) -> LocalBoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// A handler that silently ignores all errors that can never happen (e.g.:
/// [`!`] or [`Infallible`]).
///
/// ## Examples
/// ```
/// # #[compio::main]
/// # async fn main_() {
/// use std::convert::{Infallible, TryInto};
///
/// use teloxide::error_handlers::{ErrorHandler, IgnoringErrorHandlerSafe};
///
/// let result: Result<String, Infallible> = "str".try_into();
/// match result {
///     Ok(string) => println!("{}", string),
///     Err(inf) => IgnoringErrorHandlerSafe::new().handle_error(inf).await,
/// }
///
/// IgnoringErrorHandlerSafe::new().handle_error(return).await; // return type of `return` is `!` (aka never)
/// # }
/// ```
///
/// ```compile_fail
/// use teloxide::dispatching::{ErrorHandler, IgnoringErrorHandlerSafe};
///
/// IgnoringErrorHandlerSafe.handle_error(0);
/// ```
///
/// [`!`]: https://doc.rust-lang.org/std/primitive.never.html
/// [`Infallible`]: std::convert::Infallible
#[derive(Clone, Copy)]
pub struct IgnoringErrorHandlerSafe;

impl IgnoringErrorHandlerSafe {
    #[must_use]
    pub fn new() -> Rc<Self> {
        Rc::new(Self)
    }
}

#[allow(unreachable_code)]
impl ErrorHandler<Infallible> for IgnoringErrorHandlerSafe {
    fn handle_error(self: Rc<Self>, _: Infallible) -> LocalBoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// A handler that log all errors passed into it.
///
/// ## Example
/// ```
/// # #[compio::main]
/// # async fn main_() {
/// use teloxide::error_handlers::{ErrorHandler, LoggingErrorHandler};
///
/// LoggingErrorHandler::new().handle_error(()).await;
/// LoggingErrorHandler::with_custom_text("Omg1").handle_error(404).await;
/// LoggingErrorHandler::with_custom_text("Omg2").handle_error("Invalid data type!").await;
/// # }
/// ```
pub struct LoggingErrorHandler {
    text: String,
}

impl LoggingErrorHandler {
    /// Creates `LoggingErrorHandler` with a meta text before a log.
    ///
    /// The logs will be printed in this format: `{text}: {:?}`.
    #[must_use]
    pub fn with_custom_text<T>(text: T) -> Rc<Self>
    where
        T: Into<String>,
    {
        Rc::new(Self { text: text.into() })
    }

    /// A shortcut for
    /// `LoggingErrorHandler::with_custom_text("Error".to_owned())`.
    #[must_use]
    pub fn new() -> Rc<Self> {
        Self::with_custom_text("Error".to_owned())
    }
}

impl<E> ErrorHandler<E> for LoggingErrorHandler
where
    E: Debug,
{
    fn handle_error(self: Rc<Self>, error: E) -> LocalBoxFuture<'static, ()> {
        log::error!("{text}: {:?}", error, text = self.text);
        Box::pin(async {})
    }
}
