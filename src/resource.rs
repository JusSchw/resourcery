use std::{convert::Infallible, future::Future, pin::Pin};

use crate::{Placement, ResourceContext};

pub(crate) type BoxTask<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'static>>;
pub(crate) type TaskFactory<E> = Box<dyn FnOnce(ResourceContext) -> BoxTask<E> + Send>;

pub trait Resource: Send + Sync + Sized + 'static {
    type Input: Send + 'static;
    type Error: Send + Sync + 'static;
    type Placement: Placement<Self>;

    fn build(input: Self::Input) -> ResourceSpec<Self, Self::Error>;
}

pub struct ResourceSpec<R, E> {
    pub(crate) interface: R,
    pub(crate) task: TaskFactory<E>,
}

impl<R, E> ResourceSpec<R, E> {
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

pub type Never = Infallible;
