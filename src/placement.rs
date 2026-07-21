use std::{hash::Hash, marker::PhantomData};

use crate::Resource;

/// Maps a resource's creation input to its identity within a domain.
///
/// Most users select [`Unique`], [`Singleton`], or [`Keyed`] rather than
/// implementing this trait directly.
pub trait Placement<R: Resource>: Send + Sync + 'static {
    /// The key used by the per-type registry.
    type Key: Clone + Eq + Hash + Send + Sync + 'static;
    /// Whether this placement has a canonical identity that can be reused.
    const CANONICAL: bool;
    /// Derives the registry key from creation input.
    fn placement_key(input: &R::Input) -> Self::Key;
}

/// Marker for placements whose active generation can be queried and reused.
///
/// This bound enables [`ResourceContext::get`](crate::ResourceContext::get),
/// [`ResourceContext::status`](crate::ResourceContext::status), and
/// [`ResourceContext::get_or_spawn`](crate::ResourceContext::get_or_spawn).
pub trait CanonicalPlacement<R: Resource>: Placement<R> {
    /// Derives the resource's canonical key from creation input.
    fn key(input: &R::Input) -> Self::Key;
}

/// Placement in which every [`spawn`](crate::ResourceContext::spawn) creates an
/// independent generation.
pub struct Unique;

impl<R: Resource<Placement = Self>> Placement<R> for Unique {
    type Key = ();
    const CANONICAL: bool = false;
    fn placement_key(_: &R::Input) {}
}

/// Placement with at most one active generation per resource type and domain.
pub struct Singleton;

impl<R: Resource<Placement = Self>> Placement<R> for Singleton {
    type Key = ();
    const CANONICAL: bool = true;
    fn placement_key(_: &R::Input) {}
}

impl<R: Resource<Placement = Self>> CanonicalPlacement<R> for Singleton {
    fn key(_: &R::Input) {}
}

/// Derives a [`Keyed`] resource's canonical identity from its creation input.
pub trait Keyer<R: Resource>: Send + Sync + 'static {
    /// The canonical key type.
    type Key: Clone + Eq + Hash + Send + Sync + 'static;
    /// Returns the identity-bearing portion of `input`.
    fn key(input: &R::Input) -> Self::Key;
}

/// Placement with at most one active generation per key, resource type, and
/// domain.
///
/// `K` implements [`Keyer`] and controls which parts of the input participate
/// in identity.
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
