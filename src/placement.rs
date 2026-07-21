use std::{hash::Hash, marker::PhantomData};

use crate::Resource;

pub trait Placement<R: Resource>: Send + Sync + 'static {
    type Key: Clone + Eq + Hash + Send + Sync + 'static;
    const CANONICAL: bool;
    fn placement_key(input: &R::Input) -> Self::Key;
}

pub trait CanonicalPlacement<R: Resource>: Placement<R> {
    fn key(input: &R::Input) -> Self::Key;
}

pub struct Unique;

impl<R: Resource<Placement = Self>> Placement<R> for Unique {
    type Key = ();
    const CANONICAL: bool = false;
    fn placement_key(_: &R::Input) {}
}

pub struct Singleton;

impl<R: Resource<Placement = Self>> Placement<R> for Singleton {
    type Key = ();
    const CANONICAL: bool = true;
    fn placement_key(_: &R::Input) {}
}

impl<R: Resource<Placement = Self>> CanonicalPlacement<R> for Singleton {
    fn key(_: &R::Input) {}
}

pub trait Keyer<R: Resource>: Send + Sync + 'static {
    type Key: Clone + Eq + Hash + Send + Sync + 'static;
    fn key(input: &R::Input) -> Self::Key;
}

pub struct Keyed<K>(PhantomData<fn() -> K>);

impl<R, K> Placement<R> for Keyed<K>
where
    R: Resource<Placement = Self>,
    K: Keyer<R>,
{
    type Key = K::Key;
    const CANONICAL: bool = true;
    fn placement_key(input: &R::Input) -> Self::Key {
        K::key(input)
    }
}

impl<R, K> CanonicalPlacement<R> for Keyed<K>
where
    R: Resource<Placement = Self>,
    K: Keyer<R>,
{
    fn key(input: &R::Input) -> Self::Key {
        K::key(input)
    }
}
