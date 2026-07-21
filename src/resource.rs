use std::{convert::Infallible, future::Future, pin::Pin};

use crate::{Placement, ResourceContext};

pub(crate) type BoxTask<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'static>>;
pub(crate) type TaskFactory<E> = Box<dyn FnOnce(ResourceContext) -> BoxTask<E> + Send>;

/// A typed interface backed by a managed asynchronous task.
pub trait Resource: Send + Sync + Sized + 'static {
    /// Values required to establish a new resource generation.
    type Input: Send + 'static;
    /// Errors returned by the managed task after acquisition succeeds.
    type Error: Send + Sync + 'static;
    /// The policy that determines whether and how generations are shared.
    type Placement: Placement<Self>;

    /// Constructs the public interface and managed task for one generation.
    ///
    /// This method is synchronous. Expensive initialization should be performed
    /// by the task or delegated through [`ResourceContext::compute`].
    fn build(input: Self::Input) -> ResourceSpec<Self, Self::Error>;
}

/// The interface and task that make up one resource generation.
pub struct ResourceSpec<R, E> {
    pub(crate) interface: R,
    pub(crate) task: TaskFactory<E>,
}

impl<R, E> ResourceSpec<R, E> {
    /// Joins `interface` to the asynchronous task that implements it.
    ///
    /// The task starts after the resource generation has been established. It
    /// should use the supplied context to observe cancellation and acquire any
    /// resource dependencies it needs to retain.
    pub fn new<F, Fut>(interface: R, task: F) -> Self
    where
        F: FnOnce(ResourceContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        Self {
            interface,
            task: Box::new(move |cx| Box::pin(task(cx))),
        }
    }
}

/// Convenient error type for resources that cannot fail.
pub type Never = Infallible;
