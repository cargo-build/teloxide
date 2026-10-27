//! An implementation of [dependency injection].
//!
//! If you do not know what is dependency injection (DI), please read [this
//! discussion on StackOverflow], then come back. The only difference is that in
//! `dptree`, we inject objects into function-handlers, not into objects.
//!
//! Currently, the only container is [`DependencyMap`]. It implements the DI
//! pattern completely, but be careful: it can panic when you do not provide
//! necessary types. Most the times, it is profitable to use
//! [`crate::type_check`] to make sure that all types are provided _before_
//! executing a particular handler.
//!
//! [dependency injection]: https://en.wikipedia.org/wiki/Dependency_injection
//! [this discussion on StackOverflow]: https://stackoverflow.com/questions/130794/what-is-dependency-injection

use futures::future::{ready, LocalBoxFuture};

use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter, Write},
    future::Future,
    iter::FromIterator,
    panic::Location,
    rc::Rc,
};

use crate::Type;

/// A DI container with multiple dependencies.
///
/// This DI container stores types by their corresponding type identifiers. It
/// cannot prove at compile-time that a type of a requested value exists within
/// the container, so if you do not provide necessary types but they were
/// requested, it will panic. However, you can use [`crate::type_check`] to make
/// sure that all types are provided _before_ executing a particular handler.
///
/// Dependencies are stored as `Rc<dyn Any>`, so non-`Send`/`Sync` values can be
/// injected into local handlers.
///
/// # Examples
///
/// ```
/// use dptree::di::DependencyMap;
/// use std::rc::Rc;
///
/// let mut container = DependencyMap::new();
/// container.insert(5_i32);
/// container.insert("abc");
///
/// assert_eq!(container.get(), Rc::new(5_i32));
/// assert_eq!(container.get(), Rc::new("abc"));
///
/// // If a type of a value already exists within the container, it will be replaced.
/// let old_value = container.insert(10_i32).unwrap();
///
/// assert_eq!(old_value, Rc::new(5_i32));
/// assert_eq!(container.get(), Rc::new(10_i32));
/// ```
///
/// When a value is not found within the container, it will panic:
///
/// ```should_panic
/// use dptree::di::DependencyMap;
/// use std::rc::Rc;
///
/// let mut container = DependencyMap::new();
/// container.insert(10i32);
/// container.insert(true);
/// container.insert("static str");
///
/// // `alloc::string::String` was requested, but not provided. Available types:
/// //     `i32`
/// //     `&str`
/// //     `bool`
/// let string: Rc<String> = container.get();
/// ```
#[derive(Default, Clone, Debug)]
pub struct DependencyMap {
    pub(crate) map: BTreeMap<TypeId, Dependency>,
}

#[derive(Clone)]
pub(crate) struct Dependency {
    pub(crate) type_name: &'static str,
    inner: Rc<dyn Any>,
}

impl Debug for Dependency {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dependency").field("type_name", &self.type_name).finish_non_exhaustive()
    }
}

impl PartialEq for DependencyMap {
    fn eq(&self, other: &Self) -> bool {
        let keys1 = self.map.keys();
        let keys2 = other.map.keys();
        keys1.len() == keys2.len() && keys1.zip(keys2).all(|(k1, k2)| k1 == k2)
    }
}

impl DependencyMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a value into the container.
    ///
    /// If the container do not has this type present, `None` is returned.
    /// Otherwise, the value is updated, and the old value is returned.
    pub fn insert<T: 'static>(&mut self, item: T) -> Option<Rc<T>> {
        self.map
            .insert(
                TypeId::of::<T>(),
                Dependency { type_name: std::any::type_name::<T>(), inner: Rc::new(item) },
            )
            .map(|dep| dep.inner.downcast().expect("Values are stored by `TypeId`"))
    }

    /// Inserts all dependencies from another container into itself.
    pub fn insert_container(&mut self, container: Self) {
        self.map.extend(container.map);
    }

    /// Removes a value from the container.
    ///
    /// If the container do not has this type present, `None` is returned.
    /// Otherwise, the value is removed and returned.
    pub fn remove<T: 'static>(&mut self) -> Option<Rc<T>> {
        self.map
            .remove(&TypeId::of::<T>())
            .map(|dep| dep.inner.downcast().expect("Values are stored by `TypeId`"))
    }

    /// Retrieves the value of type `V` from this container.
    ///
    /// ## Panics
    ///
    /// If the container has no value of type `V`.
    pub fn get<V>(&self) -> Rc<V>
    where
        V: 'static,
    {
        self.map
            .get(&TypeId::of::<V>())
            .unwrap_or_else(|| {
                panic!(
                    "`{}` was requested, but not provided. Available types:\n{}",
                    std::any::type_name::<V>(),
                    self.available_types()
                )
            })
            .clone()
            .inner
            .downcast::<V>()
            .expect("Checked by `unwrap_or_else`")
    }

    /// Tries to get a value from the container; does not panic.
    pub fn try_get<V: 'static>(&self) -> Option<Rc<V>> {
        self.map
            .get(&TypeId::of::<V>())
            .cloned()
            .map(|v| v.inner.downcast().expect("Values are stored by `TypeId`"))
    }

    fn available_types(&self) -> String {
        let mut list = String::new();

        for dep in self.map.values() {
            writeln!(list, "    `{}`", dep.type_name).unwrap();
        }

        list
    }
}

/// Converts functions into [`CompiledFn`].
///
/// For a function to be convertible into [`CompiledFn`], it must be of 0-12
/// arguments and return [`Future`].
pub trait Injectable<Output, FnArgs>
where
    Output: 'static,
{
    fn inject<'a>(&'a self, container: &'a DependencyMap) -> CompiledFn<'a, Output>;

    /// Returns the set of types that this function depends on. Used only for
    /// run-time "type checking".
    fn input_types() -> BTreeSet<Type>;

    /// Returns the map of obligations of this function.
    ///
    /// The map contains all types from [`Self::input_types`] as keys, each one
    /// corresponding to the current [`Location::caller`].
    ///
    /// [`Location::caller`]: std::panic::Location::caller
    #[track_caller]
    fn obligations() -> BTreeMap<Type, &'static Location<'static>> {
        let location = Location::caller();
        Self::input_types().into_iter().map(|ty| (ty, location)).collect()
    }
}

/// A function with all dependencies satisfied.
pub type CompiledFn<'a, Output> = Rc<dyn Fn() -> LocalBoxFuture<'a, Output> + 'a>;

/// Turns a synchronous function into a type that implements [`Injectable`].
pub struct Asyncify<F>(pub F);

macro_rules! impl_into_di {
    ($($generic:ident),*) => {
        impl<Func, Output, Fut, $($generic),*> Injectable<Output, ($($generic,)*)> for Func
        where
            Func: Fn($($generic),*) -> Fut + 'static,
            Fut: Future<Output = Output> + 'static,
            Output: 'static,
            $($generic: Clone + 'static),*
        {
            #[allow(non_snake_case)]
            #[allow(unused_variables)]
            fn inject<'a>(&'a self, container: &'a DependencyMap) -> CompiledFn<'a, Output> {
                Rc::new(move || {
                    $(let $generic = std::borrow::Borrow::<$generic>::borrow(&container.get()).clone();)*
                    let fut = self( $( $generic ),* );
                    Box::pin(fut)
                })
            }

            fn input_types() -> BTreeSet<Type> {
                BTreeSet::from_iter(vec![
                    $(Type::of::<$generic>()),*
                ])
            }
        }

        impl<Func, Output, $($generic),*> Injectable<Output, ($($generic,)*)> for Asyncify<Func>
        where
            Func: Fn($($generic),*) -> Output + 'static,
            Output: 'static,
            $($generic: Clone + 'static),*
        {
            #[allow(non_snake_case)]
            #[allow(unused_variables)]
            fn inject<'a>(&'a self, container: &'a DependencyMap) -> CompiledFn<'a, Output> {
                let Asyncify(this) = self;
                Rc::new(move || {
                    $(let $generic = std::borrow::Borrow::<$generic>::borrow(&container.get()).clone();)*
                    let out = this( $( $generic ),* );
                    Box::pin(ready(out))
                })
            }

            fn input_types() -> BTreeSet<Type> {
                BTreeSet::from_iter(vec![
                    $(Type::of::<$generic>()),*
                ])
            }
        }
    };
}

impl_into_di!();
impl_into_di!(T1);
impl_into_di!(T1, T2);
impl_into_di!(T1, T2, T3);
impl_into_di!(T1, T2, T3, T4);
impl_into_di!(T1, T2, T3, T4, T5);
impl_into_di!(T1, T2, T3, T4, T5, T6);
impl_into_di!(T1, T2, T3, T4, T5, T6, T7);
impl_into_di!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_into_di!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_into_di!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_into_di!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_into_di!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

/// Constructs [`DependencyMap`] with a list of dependencies.
///
/// # Examples
///
/// ```
/// use dptree::di::DependencyMap;
///
/// let map = dptree::deps![123, "abc", true];
///
/// let i: i32 = *map.get();
/// let str: &str = *map.get();
/// let b: bool = *map.get();
///
/// assert!(i == 123);
/// assert!(str == "abc");
/// assert!(b == true);
/// ```
#[macro_export]
macro_rules! deps {
    ($($dep:expr),*) => {
        {
            // In the case if this macro receives zero arguments.
            #[allow(unused_mut)]
            let mut map = $crate::di::DependencyMap::new();
            $(map.insert($dep);)*
            map
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, ops::ControlFlow, rc::Rc, time::Duration};

    use super::*;

    #[test]
    fn get() {
        let mut map = DependencyMap::new();
        map.insert(42i32);
        map.insert("hello world");
        map.insert_container(deps![true]);

        assert_eq!(map.get(), Rc::new(42i32));
        assert_eq!(map.get(), Rc::new("hello world"));
        assert_eq!(map.get(), Rc::new(true));
    }

    #[test]
    fn try_get() {
        let mut map = DependencyMap::new();
        assert_eq!(map.try_get::<i32>(), None);
        map.insert(42i32);
        assert_eq!(map.try_get(), Some(Rc::new(42i32)));
        assert_eq!(map.try_get::<f32>(), None);
    }

    #[test]
    fn same_keys() {
        let mut map_bool1 = DependencyMap::new();
        let mut map_bool2 = DependencyMap::new();
        let map_empty = DependencyMap::new();

        map_bool1.insert(false);
        map_bool2.insert(true);

        assert_eq!(map_bool1, map_bool2);
        assert_ne!(map_bool1, map_empty);
        assert_ne!(map_bool2, map_empty);
    }

    // Regression test for the local (single-threaded) contract: a non-`Send`
    // `Rc` dependency is injected into an async endpoint that awaits a compio
    // timer.
    #[compio::test]
    async fn injects_rc_dependency_into_local_async_endpoint() {
        #[derive(Clone)]
        struct Counter(Rc<Cell<u32>>);

        let counter = Counter(Rc::new(Cell::new(0)));
        let observed = Rc::clone(&counter.0);

        let handler: crate::Handler<'static, &'static str> =
            crate::entry().endpoint(move |dep: Counter| async move {
                compio::time::sleep(Duration::from_millis(5)).await;
                dep.0.set(dep.0.get() + 1);
                "injected"
            });

        let result = handler.dispatch(deps![counter]).await;

        assert_eq!(result, ControlFlow::Break("injected"));
        assert_eq!(observed.get(), 1);
    }
}
