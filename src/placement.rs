use std::{hash::Hash, marker::PhantomData};

use crate::Resource;

/// Determines the canonical identity of a resource input.
pub trait Placement<R: Resource>: Send + Sync + 'static {
    /// The hashable identity used by the domain registry.
    type Key: Clone + Eq + Hash + Send + Sync + 'static;
    #[doc(hidden)]
    const CANONICAL: bool;
    #[doc(hidden)]
    fn placement_key(input: &R::Input) -> Self::Key;
}

/// A placement whose generations have a discoverable canonical identity.
pub trait CanonicalPlacement<R: Resource>: Placement<R> {
    /// Derives canonical identity from creation input.
    fn key(input: &R::Input) -> Self::Key;
}

/// Every spawn produces an independent resource generation.
pub struct Unique;

impl<R: Resource<Placement = Self>> Placement<R> for Unique {
    type Key = ();
    const CANONICAL: bool = false;
    fn placement_key(_: &R::Input) {}
}

/// One live resource generation per domain.
pub struct Singleton;

impl<R: Resource<Placement = Self>> Placement<R> for Singleton {
    type Key = ();
    const CANONICAL: bool = true;
    fn placement_key(_: &R::Input) {}
}

impl<R: Resource<Placement = Self>> CanonicalPlacement<R> for Singleton {
    fn key(_: &R::Input) {}
}

/// Derives a canonical resource key from its creation input.
pub trait Keyer<R: Resource>: Send + Sync + 'static {
    /// The immutable identity of a keyed resource.
    type Key: Clone + Eq + Hash + Send + Sync + 'static;
    /// Derives the identity of `R` from its creation input.
    fn key(input: &R::Input) -> Self::Key;
}

/// One live resource generation for each key produced by `K`.
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
