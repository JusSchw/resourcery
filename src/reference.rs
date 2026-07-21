use std::{
    ops::Deref,
    sync::{Arc, Weak},
};

use tokio::sync::watch;

use crate::{Resource, domain::Domain, lifecycle::Control};

/// The immutable terminal result of one resource generation.
#[derive(Debug)]
pub enum ResourceOutcome<E> {
    /// The managed task returned `Ok(())`.
    Completed,
    /// The managed task returned its declared error.
    Failed(Arc<E>),
    /// The managed task panicked with this message.
    Panicked(Arc<str>),
    /// The managed task was forcibly aborted.
    Aborted,
}

impl<E> Clone for ResourceOutcome<E> {
    fn clone(&self) -> Self {
        match self {
            Self::Completed => Self::Completed,
            Self::Failed(error) => Self::Failed(error.clone()),
            Self::Panicked(message) => Self::Panicked(message.clone()),
            Self::Aborted => Self::Aborted,
        }
    }
}

pub(crate) struct Entry<R: Resource> {
    pub(crate) interface: R,
    pub(crate) control: Arc<Control>,
    pub(crate) finished: watch::Receiver<Option<ResourceOutcome<R::Error>>>,
    pub(crate) domain: Weak<Domain>,
}

/// A strong lease on one resource generation.
///
/// Cloning adds another lease to the same generation. Dropping the final lease
/// requests cooperative cancellation exactly once. The interface is available
/// through [`Deref`]; an already-held reference remains usable while the task is
/// cancelling and after it has finished.
pub struct ResourceRef<R: Resource> {
    pub(crate) entry: Arc<Entry<R>>,
}

impl<R: Resource> Clone for ResourceRef<R> {
    fn clone(&self) -> Self {
        self.entry.control.clone_lease();
        Self {
            entry: self.entry.clone(),
        }
    }
}

impl<R: Resource> Drop for ResourceRef<R> {
    fn drop(&mut self) {
        self.entry.control.release();
    }
}

impl<R: Resource> Deref for ResourceRef<R> {
    type Target = R;
    fn deref(&self) -> &R {
        &self.entry.interface
    }
}

impl<R: Resource> ResourceRef<R> {
    /// Creates a non-owning reference to this generation.
    pub fn downgrade(&self) -> WeakResourceRef<R> {
        WeakResourceRef {
            entry: Arc::downgrade(&self.entry),
            domain: self.entry.domain.clone(),
        }
    }

    /// Creates a non-owning observer for this generation's terminal outcome.
    pub fn completion(&self) -> ResourceCompletion<R::Error> {
        ResourceCompletion {
            receiver: self.entry.finished.clone(),
        }
    }

    /// Waits for this generation's terminal outcome.
    ///
    /// Unlike [`completion`](Self::completion), this method borrows the strong
    /// reference and therefore keeps the generation leased while waiting.
    pub async fn finished(&self) -> ResourceOutcome<R::Error> {
        self.completion().wait().await
    }
}

/// A cloneable, non-owning observer of one generation's terminal outcome.
///
/// An observer does not keep the generation live. Every clone receives the
/// same immutable outcome.
pub struct ResourceCompletion<E> {
    receiver: watch::Receiver<Option<ResourceOutcome<E>>>,
}

impl<E> Clone for ResourceCompletion<E> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}

impl<E> ResourceCompletion<E> {
    /// Waits until the generation terminates and returns its outcome.
    pub async fn wait(mut self) -> ResourceOutcome<E> {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return result;
            }
            if self.receiver.changed().await.is_err() {
                return ResourceOutcome::Aborted;
            }
        }
    }
}

/// A non-owning reference to one resource generation.
///
/// Weak references are useful for back-references and optional relationships
/// that must not imply liveness.
pub struct WeakResourceRef<R: Resource> {
    entry: Weak<Entry<R>>,
    domain: Weak<Domain>,
}

impl<R: Resource> Clone for WeakResourceRef<R> {
    fn clone(&self) -> Self {
        Self {
            entry: self.entry.clone(),
            domain: self.domain.clone(),
        }
    }
}

impl<R: Resource> WeakResourceRef<R> {
    /// Attempts to acquire a strong lease.
    ///
    /// Upgrade succeeds only while the generation is active and its domain is
    /// still accepting acquisitions.
    pub fn upgrade(&self) -> Option<ResourceRef<R>> {
        let entry = self.entry.upgrade()?;
        let domain = self.domain.upgrade()?;
        if domain.try_acquire(&entry) {
            Some(ResourceRef { entry })
        } else {
            None
        }
    }
}
