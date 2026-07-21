use std::{
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, Weak},
};

use tokio::sync::watch;

use crate::{Resource, ResourceContext, lifecycle::Control};

pub(crate) struct Entry<R: Resource> {
    pub(crate) interface: R,
    pub(crate) control: Arc<Control>,
    pub(crate) finished: watch::Receiver<Option<Result<(), Arc<R::Error>>>>,
}

/// A strong lease on one resource generation.
pub struct ResourceRef<'context, R: Resource> {
    pub(crate) entry: Arc<Entry<R>>,
    pub(crate) context: PhantomData<&'context ResourceContext<'context>>,
}

impl<R: Resource> Clone for ResourceRef<'_, R> {
    fn clone(&self) -> Self {
        self.entry.control.clone_lease();
        Self {
            entry: self.entry.clone(),
            context: PhantomData,
        }
    }
}

impl<R: Resource> Drop for ResourceRef<'_, R> {
    fn drop(&mut self) {
        self.entry.control.release();
    }
}

impl<R: Resource> Deref for ResourceRef<'_, R> {
    type Target = R;
    fn deref(&self) -> &R {
        &self.entry.interface
    }
}

impl<'context, R: Resource> ResourceRef<'context, R> {
    /// Creates a non-owning reference to this generation.
    ///
    /// The weak reference does not delay cancellation. Its upgrade fails once
    /// the generation stops accepting new leases.
    pub fn downgrade(&self) -> WeakResourceRef<'context, R> {
        WeakResourceRef {
            entry: Arc::downgrade(&self.entry),
            context: PhantomData,
        }
    }

    /// Waits for this generation's terminal outcome.
    pub async fn finished(&self) -> Result<(), Arc<R::Error>> {
        let mut receiver = self.entry.finished.clone();
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            if receiver.changed().await.is_err() {
                unreachable!("the task publishes an outcome before dropping its sender");
            }
        }
    }
}

/// A non-owning, optionally upgradeable reference.
pub struct WeakResourceRef<'context, R: Resource> {
    entry: Weak<Entry<R>>,
    context: PhantomData<&'context ResourceContext<'context>>,
}

impl<R: Resource> Clone for WeakResourceRef<'_, R> {
    fn clone(&self) -> Self {
        Self {
            entry: self.entry.clone(),
            context: PhantomData,
        }
    }
}

impl<'context, R: Resource> WeakResourceRef<'context, R> {
    /// Attempts to acquire a strong lease on the referenced generation.
    ///
    /// Returns `None` if the generation was reclaimed, is cancelling, or has
    /// already finished.
    pub fn upgrade(&self) -> Option<ResourceRef<'context, R>> {
        let entry = self.entry.upgrade()?;
        entry.control.acquire().then_some(ResourceRef {
            entry,
            context: PhantomData,
        })
    }
}
