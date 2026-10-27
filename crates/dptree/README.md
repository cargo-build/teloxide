# dptree (vendored local-futures fork)

This crate is a vendored copy of [`dptree`] 0.5.1 from the official crate
tarball (`dptree-0.5.1.crate`, upstream repository [teloxide/dptree], MIT
license — see `LICENSE`).

## Provenance

- Source: official crates.io package `dptree` 0.5.1, downloaded as
  `~/.local/state/codex/teloxide-compio-20261027/reference/dptree-0.5.1`.
- Upstream: <https://github.com/teloxide/dptree> (master).
- Local adaptation for the teloxide compio/io_uring effort; the crate is
  consumed by `crates/teloxide` via `path = "../dptree"`.

## Local differences from the published 0.5.1

This fork drops the multithreaded runtime contract and targets single-threaded
compio runtimes with local (non-`Send`) futures:

- `DependencyMap` stores dependencies as `Rc<dyn Any>` instead of
  `Arc<dyn Any + Send + Sync>`; `get`/`try_get`/`insert`/`remove` traffic in
  `Rc<T>` and only require `'static`.
- `Injectable`/`CompiledFn` accept plain `Fn` closures returning
  `futures::future::LocalBoxFuture`; handler functions and their futures no
  longer need to be `Send` or `Sync`.
- `Handler`, continuations (`Cont`) and handler results (`HandlerResult`) are
  `Rc`-shared local types.
- Async tests use `#[compio::test]` (compio 0.19.2) instead of `#[tokio::test]`.

The public package name, macros (`deps!`, `case!`), combinator functions and
tests/contracts are otherwise unchanged from 0.5.1.

[`dptree`]: https://docs.rs/dptree/0.5.1
[teloxide/dptree]: https://github.com/teloxide/dptree
