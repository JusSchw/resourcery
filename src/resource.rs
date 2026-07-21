use std::{convert::Infallible, future::Future, pin::Pin};

use crate::{Placement, ResourceContext};

pub(crate) type BoxTask<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'static>>;
pub(crate) type TaskFactory<E> = Box<dyn FnOnce(ResourceContext) -> BoxTask<E> + Send>;

/// A typed public interface backed by one managed asynchronous task.
///
/// `build` is called once for each newly established generation. It constructs
/// the interface synchronously and returns the task that drives it. For a
/// canonical placement, the input's key selects an existing generation before
/// `build` is called; creation-only input is therefore ignored when a generation
/// is reused.
pub trait Resource: Send + Sync + Sized + 'static {
    /// Values supplied when creating a generation.
    type Input: Send + 'static;
    /// The task's domain-specific failure type.
    type Error: Send + Sync + 'static;
    /// The policy that maps creation requests to resource identities.
    type Placement: Placement<Self>;

    /// Constructs the public interface and managed task for a new generation.
    ///
    /// A panic from this method is reported as
    /// [`AcquireError::ConstructionPanicked`](crate::AcquireError::ConstructionPanicked).
    /// Panics from the returned task instead become
    /// [`ResourceOutcome::Panicked`](crate::ResourceOutcome::Panicked).
    fn build(input: Self::Input) -> ResourceSpec<Self, Self::Error>;
}

/// The two parts of a resource generation: its public interface and its task.
///
/// The interface is immediately available through [`ResourceRef`](crate::ResourceRef).
/// The task starts after the generation has been registered in its domain.
pub struct ResourceSpec<R, E> {
    pub(crate) interface: R,
    pub(crate) task: TaskFactory<E>,
}

impl<R, E> ResourceSpec<R, E> {
    /// Creates a specification from a public interface and a task factory.
    ///
    /// The factory receives the generation's [`ResourceContext`] and must
    /// return when the resource has completed its shutdown. Returning `Err`
    /// records a [`ResourceOutcome::Failed`](crate::ResourceOutcome::Failed)
    /// outcome.
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

/// An error type for resource tasks that cannot fail.
pub type Never = Infallible;
