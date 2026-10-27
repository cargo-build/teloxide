use crate::{
    dispatching::{
        distribution::default_distribution_function, DefaultKey, DpHandlerDescription,
        ShutdownToken,
    },
    error_handlers::{ErrorHandler, LoggingErrorHandler},
    requests::{Request, Requester},
    stop::StopToken,
    types::{Update, UpdateKind},
    update_listeners::{self, UpdateListener},
};

use dptree::di::DependencyMap;
use either::Either;
use futures::{
    future::{self, LocalBoxFuture},
    stream::FuturesUnordered,
    FutureExt as _, StreamExt as _,
};

use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    ops::{ControlFlow, Deref},
    pin::pin,
    rc::Rc,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
};

/// The builder for [`Dispatcher`].
///
/// See also: ["Dispatching or
/// REPLs?"](../dispatching/index.html#dispatching-or-repls)
pub struct DispatcherBuilder<R, Err, Key> {
    bot: R,
    dependencies: DependencyMap,
    handler: Rc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Rc<dyn ErrorHandler<Err>>,
    ctrlc_handler: bool,
    distribution_f: fn(&Update) -> Option<Key>,
    worker_queue_size: usize,
}

impl<R, Err, Key> DispatcherBuilder<R, Err, Key>
where
    R: Clone + Requester + 'static,
    Err: Debug + 'static,
{
    /// Specifies a handler that will be called for an unhandled update.
    ///
    /// By default, it is a mere [`log::warn`].
    #[must_use]
    pub fn default_handler<H, Fut>(self, handler: H) -> Self
    where
        H: Fn(Rc<Update>) -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let handler = Rc::new(handler);

        Self {
            default_handler: Rc::new(move |upd| {
                let handler = Rc::clone(&handler);
                Box::pin(handler(upd))
            }),
            ..self
        }
    }

    /// Specifies a handler that will be called on a handler error.
    ///
    /// By default, it is [`LoggingErrorHandler`].
    #[must_use]
    pub fn error_handler(self, handler: Rc<dyn ErrorHandler<Err>>) -> Self {
        Self { error_handler: handler, ..self }
    }

    /// Specifies dependencies that can be used inside of handlers.
    ///
    /// By default, there is no dependencies.
    #[must_use]
    pub fn dependencies(self, dependencies: DependencyMap) -> Self {
        Self { dependencies, ..self }
    }

    /// Enables the `^C` handler that [`shutdown`]s dispatching.
    ///
    /// [`shutdown`]: ShutdownToken::shutdown
    #[cfg(feature = "ctrlc_handler")]
    #[must_use]
    pub fn enable_ctrlc_handler(self) -> Self {
        Self { ctrlc_handler: true, ..self }
    }

    /// Specifies size of the queue for workers.
    ///
    /// By default it's 64.
    #[must_use]
    pub fn worker_queue_size(self, size: usize) -> Self {
        Self { worker_queue_size: size, ..self }
    }

    /// Specifies the stack size available to the dispatcher.
    ///
    /// By default, it's 8 * 1024 * 1024 bytes (8 MiB).
    #[must_use]
    #[deprecated(since = "0.15.0", note = "This method is a no-op; you can just remove it.")]
    pub fn stack_size(self, _size: usize) -> Self {
        self
    }

    /// Specifies the distribution function that decides how updates are grouped
    /// before execution.
    ///
    /// ## Update grouping
    ///
    /// When [`Dispatcher`] receives updates, it runs dispatching tree
    /// (handlers) concurrently. This means that multiple updates can be
    /// processed at the same time.
    ///
    /// However, this is not always convenient. For example, if you have global
    /// state, then you may want to process some updates sequentially, to
    /// prevent state inconsistencies.
    ///
    /// This is why `teloxide` allows grouping updates. Updates for which the
    /// distribution function `f` returns the same "distribution key" `K` will
    /// be run in sequence (while still being processed concurrently with the
    /// updates with different distribution keys).
    ///
    /// Updates for which `f` returns `None` will always be processed in
    /// parallel.
    ///
    /// ## Default distribution function
    ///
    /// By default the distribution function is equivalent to `|upd|
    /// upd.chat().map(|chat| chat.id)`, so updates from the same chat will be
    /// processed sequentially.
    ///
    /// This pair nicely with dialogue system, which has state attached to
    /// chats.
    ///
    /// ## Examples
    ///
    /// Grouping updates by user who caused this update to happen:
    ///
    /// ```
    /// use teloxide::{dispatching::Dispatcher, dptree, Bot};
    ///
    /// let bot = Bot::new("TOKEN");
    /// let handler = dptree::entry() /* ... */;
    /// let dp = Dispatcher::builder(bot, handler)
    ///     .distribution_function(|upd| upd.from().map(|user| user.id))
    ///     .build();
    /// # let _: Dispatcher<_, (), _> = dp;
    /// ```
    ///
    /// Not grouping updates at all, always processing updates concurrently:
    ///
    /// ```
    /// use teloxide::{dispatching::Dispatcher, dptree, Bot};
    ///
    /// let bot = Bot::new("TOKEN");
    /// let handler = dptree::entry() /* ... */;
    /// let dp = Dispatcher::builder(bot, handler).distribution_function(|_| None::<()>).build();
    /// # let _: Dispatcher<_, (), _> = dp;
    /// ```
    #[must_use]
    pub fn distribution_function<K>(
        self,
        f: fn(&Update) -> Option<K>,
    ) -> DispatcherBuilder<R, Err, K>
    where
        K: Hash + Eq,
    {
        let Self {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            ctrlc_handler,
            distribution_f: _,
            worker_queue_size,
        } = self;

        DispatcherBuilder {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            ctrlc_handler,
            distribution_f: f,
            worker_queue_size,
        }
    }

    /// Constructs [`Dispatcher`].
    ///
    /// ## Panics
    /// This function will panic at run-time if [`dptree`] fails to type-check
    /// the provided handler. An appropriate error message will be emitted.
    #[must_use]
    pub fn build(self) -> Dispatcher<R, Err, Key> {
        let Self {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            distribution_f,
            worker_queue_size,
            ctrlc_handler,
        } = self;

        dptree::type_check(
            handler.sig(),
            &dependencies,
            &[
                dptree::Type::of::<R>(),
                dptree::Type::of::<teloxide_core::types::Update>(),
                dptree::Type::of::<teloxide_core::types::Me>(),
            ],
        );

        // If the `ctrlc_handler` feature is not enabled, don't emit a warning.
        let _ = ctrlc_handler;

        let dp = Dispatcher {
            bot,
            dependencies,
            handler,
            default_handler,
            error_handler,
            state: ShutdownToken::new(),
            distribution_f,
            worker_queue_size,
            workers: HashMap::new(),
            default_worker: None,
            current_number_of_active_workers: Default::default(),
            max_number_of_active_workers: Default::default(),
        };

        #[cfg(feature = "ctrlc_handler")]
        {
            if ctrlc_handler {
                let mut dp = dp;
                dp.setup_ctrlc_handler_inner();
                return dp;
            }
        }

        dp
    }
}

/// The base for update dispatching.
///
/// ## Update grouping
///
/// `Dispatcher` generally processes updates concurrently. However, by default,
/// updates from the same chat are processed sequentially. Learn more about
/// [update grouping].
///
/// See also: ["Dispatching or
/// REPLs?"](../dispatching/index.html#dispatching-or-repls)
///
/// [update grouping]: DispatcherBuilder#update-grouping
pub struct Dispatcher<R, Err, Key> {
    bot: R,
    dependencies: DependencyMap,

    handler: Rc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,

    distribution_f: fn(&Update) -> Option<Key>,
    worker_queue_size: usize,
    current_number_of_active_workers: Rc<AtomicU32>,
    max_number_of_active_workers: Rc<AtomicU32>,
    // Channel sender parts associated with chat IDs that consume updates
    // sequentially.
    workers: HashMap<Key, Worker>,
    // The default sender part that consumes updates concurrently.
    default_worker: Option<Worker>,

    error_handler: Rc<dyn ErrorHandler<Err>>,

    state: ShutdownToken,
}

struct Worker {
    tx: async_channel::Sender<Update>,
    handle: compio::runtime::JoinHandle<()>,
    is_waiting: Rc<AtomicBool>,
}

// TODO: it is allowed to return message as response on telegram request in
// webhooks, so we can allow this too. See more there: https://core.telegram.org/bots/api#making-requests-when-getting-updates

/// A handler that processes updates from Telegram.
pub type UpdateHandler<Err> = dptree::Handler<'static, Result<(), Err>, DpHandlerDescription>;

type DefaultHandler = Rc<dyn Fn(Rc<Update>) -> LocalBoxFuture<'static, ()>>;

impl<R, Err> Dispatcher<R, Err, DefaultKey>
where
    R: Requester + Clone + 'static,
    Err: Debug,
{
    /// Constructs a new [`DispatcherBuilder`] with `bot` and `handler`.
    #[must_use]
    pub fn builder(bot: R, handler: UpdateHandler<Err>) -> DispatcherBuilder<R, Err, DefaultKey> {
        const DEFAULT_WORKER_QUEUE_SIZE: usize = 64;

        DispatcherBuilder {
            bot,
            dependencies: DependencyMap::new(),
            handler: Rc::new(handler),
            default_handler: Rc::new(|upd| {
                log::warn!("Unhandled update: {upd:?}");
                Box::pin(async {})
            }),
            error_handler: LoggingErrorHandler::new(),
            ctrlc_handler: false,
            worker_queue_size: DEFAULT_WORKER_QUEUE_SIZE,
            distribution_f: default_distribution_function,
        }
    }
}

impl<R, Err, Key> Dispatcher<R, Err, Key>
where
    R: Clone + Requester + 'static,
    Err: 'static,
    Key: Hash + Eq + Clone,
{
    /// Starts your bot with the default parameters.
    ///
    /// The default parameters are a long polling update listener and log all
    /// errors produced by this listener.
    ///
    /// Each time a handler is invoked, [`Dispatcher`] adds the following
    /// dependencies (in addition to those passed to
    /// [`DispatcherBuilder::dependencies`]):
    ///
    ///  - Your bot passed to [`Dispatcher::builder`];
    ///  - An update from Telegram;
    ///  - [`crate::types::Me`] (can be used in [`HandlerExt::filter_command`]).
    ///
    /// [`HandlerExt::filter_command`]: crate::dispatching::HandlerExt::filter_command
    pub async fn dispatch(&mut self)
    where
        R: Requester + Clone,
    {
        let listener = update_listeners::polling_default(self.bot.clone()).await;
        let error_handler =
            LoggingErrorHandler::with_custom_text("An error from the update listener");

        self.dispatch_with_listener(listener, error_handler).await;
    }

    /// Starts your bot with custom `update_listener` and
    /// `update_listener_error_handler`.
    ///
    /// This method adds the same dependencies as [`Dispatcher::dispatch`].
    pub async fn dispatch_with_listener<'a, UListener, Eh>(
        &'a mut self,
        update_listener: UListener,
        update_listener_error_handler: Rc<Eh>,
    ) where
        UListener: UpdateListener + 'a,
        Eh: ErrorHandler<UListener::Err> + 'a,
        UListener::Err: Debug,
    {
        self.try_dispatch_with_listener(update_listener, update_listener_error_handler)
            .await
            .expect("Couldn't prepare dispatching context")
    }

    /// Same as `dispatch_with_listener` but returns a `Err(_)` instead of
    /// panicking when the initial telegram api call (`get_me`) fails.
    ///
    /// Starts your bot with custom `update_listener` and
    /// `update_listener_error_handler`.
    ///
    /// This method adds the same dependencies as [`Dispatcher::dispatch`].
    pub async fn try_dispatch_with_listener<'a, UListener, Eh>(
        &'a mut self,
        mut update_listener: UListener,
        update_listener_error_handler: Rc<Eh>,
    ) -> Result<(), R::Err>
    where
        UListener: UpdateListener + 'a,
        Eh: ErrorHandler<UListener::Err> + 'a,
        UListener::Err: Debug,
    {
        // FIXME: there should be a way to check if dependency is already
        // inserted
        let me = self.bot.get_me().send().await?;
        self.dependencies.insert(me);
        self.dependencies.insert(self.bot.clone());

        let description = self.handler.description();
        let allowed_updates = description.allowed_updates();
        log::debug!("hinting allowed updates: {allowed_updates:?}");
        update_listener.hint_allowed_updates(&mut allowed_updates.into_iter());

        let stop_token = Some(update_listener.stop_token());
        self.start_listening(update_listener, update_listener_error_handler, stop_token).await;

        Ok(())
    }
}

impl<R, Err, Key> Dispatcher<R, Err, Key>
where
    Err: 'static,
    Key: Hash + Eq + Clone,
{
    async fn start_listening<'a, UListener, Eh>(
        &'a mut self,
        mut update_listener: UListener,
        update_listener_error_handler: Rc<Eh>,
        mut stop_token: Option<StopToken>,
    ) where
        UListener: UpdateListener + 'a,
        Eh: ErrorHandler<UListener::Err> + 'a,
        UListener::Err: Debug,
    {
        self.state.start_dispatching();

        let stream = update_listener.as_stream();
        let mut stream = pin!(stream);

        loop {
            self.remove_inactive_workers_if_needed().await;
            if self.state.is_shutting_down() {
                if let Some(token) = stop_token.take() {
                    token.stop();
                }
            }

            let res = future::select(stream.next(), pin!(self.state.wait_for_changes()))
                .map(either)
                .await
                .map_either(|l| l.0, |r| r.0);

            match res {
                Either::Left(upd) => match upd {
                    Some(upd) => self.process_update(upd, &update_listener_error_handler).await,
                    None => break,
                },
                Either::Right(()) => {
                    if self.state.is_shutting_down() {
                        if let Some(token) = stop_token.take() {
                            log::debug!("Start shutting down dispatching...");
                            token.stop();
                        }
                    }
                }
            }
        }

        self.workers
            .drain()
            .map(|(_chat_id, worker)| worker.handle)
            .chain(self.default_worker.take().map(|worker| worker.handle))
            .collect::<FuturesUnordered<_>>()
            .for_each(|res| async {
                res.expect("Failed to wait for a worker.");
            })
            .await;

        self.state.done();
    }

    async fn process_update<LErr, LErrHandler>(
        &mut self,
        update: Result<Update, LErr>,
        err_handler: &Rc<LErrHandler>,
    ) where
        LErrHandler: ErrorHandler<LErr>,
    {
        match update {
            Ok(upd) => {
                if let UpdateKind::Error(err) = upd.kind {
                    log::error!(
                        "Cannot parse an update.\nError: {err:?}\n\
                            This is a bug in teloxide-core, please open an issue here: \
                            https://github.com/teloxide/teloxide/issues.",
                    );
                    return;
                }

                let worker = match (self.distribution_f)(&upd) {
                    Some(key) => self.workers.entry(key).or_insert_with(|| {
                        let deps = self.dependencies.clone();
                        let handler = Rc::clone(&self.handler);
                        let default_handler = Rc::clone(&self.default_handler);
                        let error_handler = Rc::clone(&self.error_handler);

                        spawn_worker(
                            deps,
                            handler,
                            default_handler,
                            error_handler,
                            Rc::clone(&self.current_number_of_active_workers),
                            Rc::clone(&self.max_number_of_active_workers),
                            self.worker_queue_size,
                        )
                    }),
                    None => self.default_worker.get_or_insert_with(|| {
                        let deps = self.dependencies.clone();
                        let handler = Rc::clone(&self.handler);
                        let default_handler = Rc::clone(&self.default_handler);
                        let error_handler = Rc::clone(&self.error_handler);

                        spawn_default_worker(
                            deps,
                            handler,
                            default_handler,
                            error_handler,
                            self.worker_queue_size,
                        )
                    }),
                };

                worker.tx.send(upd).await.expect("TX is dead");
            }
            Err(err) => err_handler.clone().handle_error(err).await,
        }
    }

    async fn remove_inactive_workers_if_needed(&mut self) {
        let workers = self.workers.len();
        let max = self.max_number_of_active_workers.load(Ordering::Relaxed) as usize;

        if workers <= max {
            return;
        }

        self.remove_inactive_workers().await;
    }

    #[inline(never)] // Cold function.
    async fn remove_inactive_workers(&mut self) {
        let handles = self
            .workers
            .iter()
            .filter(|(_, worker)| worker.tx.is_empty() && worker.is_waiting.load(Ordering::Relaxed))
            .map(|(k, _)| k)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|key| {
                let Worker { tx, handle, .. } = self.workers.remove(&key).unwrap();

                // Close channel, worker should stop almost immediately
                // (it's been supposedly waiting on the channel)
                drop(tx);

                handle
            });

        for handle in handles {
            // We must wait for worker to stop anyway, even though it should
            // stop immediately. This helps in case if we've checked that the
            // worker is waiting in between it received the update and set the
            // flag.
            let _ = handle.await;
        }
    }

    /// Returns a shutdown token, which can later be used to
    /// [`ShutdownToken::shutdown`].
    pub fn shutdown_token(&self) -> ShutdownToken {
        self.state.clone()
    }
}

impl<R, Err, Key> Dispatcher<R, Err, Key> {
    #[cfg(feature = "ctrlc_handler")]
    fn setup_ctrlc_handler_inner(&mut self) {
        let token = self.state.clone();
        let handle = compio::runtime::spawn(async move {
            loop {
                compio::signal::ctrl_c().await.expect("Failed to listen for ^C");

                match token.shutdown() {
                    Ok(f) => {
                        log::info!("^C received, trying to shutdown the dispatcher...");
                        f.await;
                        log::info!("dispatcher is shutdown...");
                    }
                    Err(_) => {
                        log::info!("^C received, the dispatcher isn't running, ignoring the signal")
                    }
                }
            }
        });
        // The signal listener intentionally outlives this handle: dropping a
        // compio JoinHandle cancels the task.
        handle.detach();
    }
}

fn spawn_worker<Err>(
    deps: DependencyMap,
    handler: Rc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Rc<dyn ErrorHandler<Err>>,
    current_number_of_active_workers: Rc<AtomicU32>,
    max_number_of_active_workers: Rc<AtomicU32>,
    queue_size: usize,
) -> Worker
where
    Err: 'static,
{
    let (tx, rx) = async_channel::bounded(queue_size);
    let is_waiting = Rc::new(AtomicBool::new(true));
    let is_waiting_local = Rc::clone(&is_waiting);

    let deps = Rc::new(deps);

    let handle = compio::runtime::spawn(async move {
        while let Ok(update) = rx.recv().await {
            is_waiting_local.store(false, Ordering::Relaxed);
            {
                let current = current_number_of_active_workers.fetch_add(1, Ordering::Relaxed) + 1;
                max_number_of_active_workers.fetch_max(current, Ordering::Relaxed);
            }

            let deps = Rc::clone(&deps);
            let handler = Rc::clone(&handler);
            let default_handler = Rc::clone(&default_handler);
            let error_handler = Rc::clone(&error_handler);

            handle_update(update, deps, handler, default_handler, error_handler).await;

            current_number_of_active_workers.fetch_sub(1, Ordering::Relaxed);
            is_waiting_local.store(true, Ordering::Relaxed);
        }
    });

    Worker { tx, handle, is_waiting }
}

fn spawn_default_worker<Err>(
    deps: DependencyMap,
    handler: Rc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Rc<dyn ErrorHandler<Err>>,
    queue_size: usize,
) -> Worker
where
    Err: 'static,
{
    let (tx, rx) = async_channel::bounded(queue_size);

    let deps = Rc::new(deps);

    let handle = compio::runtime::spawn(rx.for_each_concurrent(None, move |update| {
        let deps = Rc::clone(&deps);
        let handler = Rc::clone(&handler);
        let default_handler = Rc::clone(&default_handler);
        let error_handler = Rc::clone(&error_handler);

        handle_update(update, deps, handler, default_handler, error_handler)
    }));

    Worker { tx, handle, is_waiting: Rc::new(AtomicBool::new(true)) }
}

async fn handle_update<Err>(
    update: Update,
    deps: Rc<DependencyMap>,
    handler: Rc<UpdateHandler<Err>>,
    default_handler: DefaultHandler,
    error_handler: Rc<dyn ErrorHandler<Err>>,
) where
    Err: 'static,
{
    let mut deps = deps.deref().clone();
    deps.insert(update);

    match handler.dispatch(deps).await {
        ControlFlow::Break(Ok(())) => (),
        ControlFlow::Break(Err(err)) => error_handler.clone().handle_error(err).await,
        ControlFlow::Continue(deps) => {
            let update = deps.get();
            (default_handler)(update).await;
        }
    }
}

fn either<L, R>(x: future::Either<L, R>) -> Either<L, R> {
    match x {
        future::Either::Left(l) => Either::Left(l),
        future::Either::Right(r) => Either::Right(r),
    }
}
#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        convert::Infallible,
        pin::Pin,
        task::{self, Poll},
        time::Duration,
    };

    use chrono::DateTime;
    use futures::Stream;
    use teloxide_core::types::{
        Chat, ChatId, ChatKind, ChatPrivate, LinkPreviewOptions, MediaKind, MediaText, Message,
        MessageCommon, MessageId, MessageKind, Update, UpdateId, UpdateKind, User, UserId,
    };

    use super::*;
    use crate::{
        error_handlers::IgnoringErrorHandler, stop::mk_stop_token, update_listeners::AsUpdateStream,
    };

    /// An update listener that yields a scripted list of updates and keeps
    /// pending forever until its stop token is used.
    struct ScriptedListener {
        updates: VecDeque<Update>,
        flag: crate::stop::StopFlag,
        token: StopToken,
    }

    impl crate::update_listeners::UpdateListener for ScriptedListener {
        type Err = Infallible;

        fn stop_token(&mut self) -> StopToken {
            self.token.clone()
        }
    }

    impl<'a> AsUpdateStream<'a> for ScriptedListener {
        type StreamErr = Infallible;
        type Stream = ScriptedStream;

        fn as_stream(&mut self) -> Self::Stream {
            ScriptedStream { updates: std::mem::take(&mut self.updates), flag: self.flag.clone() }
        }
    }

    struct ScriptedStream {
        updates: VecDeque<Update>,
        flag: crate::stop::StopFlag,
    }

    impl Stream for ScriptedStream {
        type Item = Result<Update, Infallible>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();

            if let Some(upd) = this.updates.pop_front() {
                return Poll::Ready(Some(Ok(upd)));
            }

            // End the stream only after the stop flag is raised.
            if Pin::new(&mut this.flag).poll(cx).is_ready() {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        }
    }

    fn make_update(chat_id: i64, update_id: u32, text: String) -> Update {
        let date = DateTime::from_timestamp(1_569_518_829, 0).unwrap();
        Update {
            id: UpdateId(update_id),
            kind: UpdateKind::Message(Message {
                via_bot: None,
                id: MessageId(update_id.try_into().unwrap()),
                thread_id: None,
                from: Some(User {
                    id: UserId(109_998_024),
                    is_bot: false,
                    first_name: String::from("Laster"),
                    last_name: None,
                    username: Some(String::from("laster_alex")),
                    language_code: Some(String::from("en")),
                    is_premium: false,
                    added_to_attachment_menu: false,
                }),
                sender_chat: None,
                is_topic_message: false,
                is_paid_post: false,
                suggested_post_info: None,
                sender_business_bot: None,
                date,
                chat: Chat {
                    id: ChatId(chat_id),
                    kind: ChatKind::Private(ChatPrivate {
                        username: Some(String::from("Laster")),
                        first_name: Some(String::from("laster_alex")),
                        last_name: None,
                    }),
                },
                direct_messages_topic: None,
                kind: MessageKind::Common(MessageCommon {
                    reply_to_message: None,
                    forward_origin: None,
                    external_reply: None,
                    quote: None,
                    edit_date: None,
                    media_kind: MediaKind::Text(MediaText {
                        text,
                        entities: vec![],
                        link_preview_options: Some(LinkPreviewOptions {
                            is_disabled: true,
                            url: None,
                            prefer_small_media: false,
                            prefer_large_media: false,
                            show_above_text: false,
                        }),
                    }),
                    reply_markup: None,
                    author_signature: None,
                    paid_star_count: None,
                    effect_id: None,
                    is_automatic_forward: false,
                    has_protected_content: false,
                    reply_to_checklist_task_id: None,
                    reply_to_story: None,
                    sender_boost_count: None,
                    is_from_offline: false,
                    business_connection_id: None,
                }),
            }),
        }
    }

    // Updates from one distribution key (chat) must be processed sequentially,
    // while updates from different keys run concurrently; a shutdown must
    // drain all updates already queued to workers.
    #[compio::test]
    async fn dispatcher_preserves_per_key_ordering_and_drains_on_stop() {
        let chat_a = ChatId(1);
        let chat_b = ChatId(2);
        // Interleaved updates: A1, B1, A2, B2, A3, B3 (by update id).
        let updates = vec![
            make_update(chat_a.0, 1, "a1".to_owned()),
            make_update(chat_b.0, 2, "b1".to_owned()),
            make_update(chat_a.0, 3, "a2".to_owned()),
            make_update(chat_b.0, 4, "b2".to_owned()),
            make_update(chat_a.0, 5, "a3".to_owned()),
            make_update(chat_b.0, 6, "b3".to_owned()),
        ];
        let total = updates.len();

        let (token, flag) = mk_stop_token();
        let mut listener = ScriptedListener { updates: updates.into(), flag, token };

        // The handler state is deliberately non-`Send` (`Rc`): the dispatcher
        // must accept local handlers.
        let processed = Rc::new(RefCell::new(Vec::<(ChatId, u32)>::new()));
        let started = Rc::new(Cell::new(0usize));

        let state = ShutdownToken::new();
        let handler_state = state.clone();
        let handler_processed = Rc::clone(&processed);
        let handler_started = Rc::clone(&started);
        let handler: UpdateHandler<Infallible> = dptree::entry().endpoint(move |upd: Update| {
            let processed = Rc::clone(&handler_processed);
            let started = Rc::clone(&handler_started);
            let state = handler_state.clone();
            async move {
                started.set(started.get() + 1);
                if started.get() == 1 {
                    let _done = state.shutdown().unwrap();
                }
                // Give other workers a chance to run concurrently.
                compio::time::sleep(Duration::from_millis(10)).await;
                let chat = upd.chat().expect("test updates carry a chat");
                processed.borrow_mut().push((chat.id, upd.id.0));
                Ok(())
            }
        });

        let mut dp = Dispatcher {
            bot: (),
            dependencies: DependencyMap::new(),
            handler: Rc::new(handler),
            default_handler: Rc::new(|upd| {
                log::warn!("Unhandled update: {upd:?}");
                Box::pin(async {})
            }),
            error_handler: IgnoringErrorHandler::new(),
            state,
            distribution_f: default_distribution_function,
            worker_queue_size: 1,
            workers: HashMap::new(),
            default_worker: None,
            current_number_of_active_workers: Rc::new(AtomicU32::new(0)),
            max_number_of_active_workers: Rc::new(AtomicU32::new(0)),
        };

        let shutdown_token = dp.shutdown_token();
        let stop_token = listener.stop_token();

        // Shutdown begins inside a handler while the dispatcher is applying
        // queue backpressure, before its next notification listener exists.
        let supervisor = compio::runtime::spawn(async move {
            while started.get() < 2 {
                compio::time::sleep(Duration::from_millis(1)).await;
            }

            let done = shutdown_token.shutdown().expect("dispatcher must be running");
            done.await;
        });

        compio::time::timeout(
            Duration::from_secs(2),
            dp.start_listening(listener, IgnoringErrorHandler::new(), Some(stop_token)),
        )
        .await
        .expect("dispatcher must stop even if the shutdown notification preceded its wait");
        supervisor.await.expect("supervisor task failed");

        let processed = processed.borrow();
        assert_eq!(processed.len(), total, "drain on stop must process queued updates");

        for chat in [chat_a, chat_b] {
            let ids: Vec<u32> =
                processed.iter().filter(|(c, _)| *c == chat).map(|(_, id)| *id).collect();
            assert_eq!(ids.len(), 3, "each chat must see all of its updates");
            assert!(
                ids.windows(2).all(|w| w[0] < w[1]),
                "updates of one distribution key must be processed sequentially: {ids:?}"
            );
        }
    }

    #[compio::test]
    async fn unkeyed_worker_runs_local_handlers_concurrently_and_drains() {
        let (started, wait_started) = async_channel::unbounded();
        let (release, wait_release) = async_channel::unbounded();
        let completed = Rc::new(Cell::new(0));
        let counter = completed.clone();
        let handler: UpdateHandler<Infallible> = dptree::entry().endpoint(move || {
            let started = started.clone();
            let wait_release = wait_release.clone();
            let completed = counter.clone();
            async move {
                started.send(()).await.unwrap();
                wait_release.recv().await.unwrap();
                completed.set(completed.get() + 1);
                Ok(())
            }
        });
        let worker = spawn_default_worker(
            DependencyMap::new(),
            Rc::new(handler),
            Rc::new(|_| Box::pin(async {})),
            IgnoringErrorHandler::new(),
            4,
        );
        for id in 1..=3 {
            worker.tx.send(make_update(1, id, String::new())).await.unwrap();
        }
        compio::time::timeout(Duration::from_secs(2), async {
            for _ in 0..3 {
                wait_started.recv().await.unwrap();
            }
        })
        .await
        .expect("unkeyed handlers must all start before any completes");
        assert_eq!(completed.get(), 0);
        for _ in 0..3 {
            release.send(()).await.unwrap();
        }
        drop(worker.tx);
        compio::time::timeout(Duration::from_secs(2), worker.handle).await.unwrap().unwrap();
        assert_eq!(completed.get(), 3);
    }
}
