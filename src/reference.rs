use std::{
    ops::Deref,
    sync::{Arc, Weak},
};

use tokio::sync::watch;

use crate::{Resource, lifecycle::Control};

/// The single terminal outcome of a managed resource generation.
#[derive(Debug)]
pub enum ResourceOutcome<E> {
    /// The resource task returned successfully.
    Completed,
    /// The resource task returned its declared error.
    Failed(Arc<E>),
    /// The resource task panicked. The string is the panic payload when known.
    Panicked(Arc<str>),
    /// The executor terminated the task before it produced a result.
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
}

/// A movable, shareable strong lease on one resource generation.
///
/// This is an ordinary runtime-domain handle, not a lexically scoped borrow.
/// Cloning an active reference creates another lease. Once its task has reached
/// a terminal outcome the remaining interface may still be read, but it cannot
/// be cloned into a new lease. Dropping the final lease starts cancellation
/// exactly once. Runtime supervision and completion observers do not contain a
/// `ResourceRef` and therefore do not count as leases.
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
    ///
    /// The weak reference does not delay cancellation. Its upgrade fails once
    /// the generation stops accepting new leases.
    pub fn downgrade(&self) -> WeakResourceRef<R> {
        WeakResourceRef {
            entry: Arc::downgrade(&self.entry),
        }
    }

    /// Creates a non-owning, clonable terminal-outcome observer.
    pub fn completion(&self) -> ResourceCompletion<R::Error> {
        ResourceCompletion {
            receiver: self.entry.finished.clone(),
        }
    }

    /// Waits for this generation's terminal outcome while retaining this lease.
    pub async fn finished(&self) -> ResourceOutcome<R::Error> {
        self.completion().wait().await
    }
}

/// A non-owning observer for one generation's terminal outcome.
///
/// It remains usable after every strong lease and the public interface have
/// been released. Clones all observe the same immutable outcome.
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
    /// Waits until the managed task reaches its one terminal state.
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

/// A non-owning, optionally upgradeable reference.
pub struct WeakResourceRef<R: Resource> {
    entry: Weak<Entry<R>>,
}

impl<R: Resource> Clone for WeakResourceRef<R> {
    fn clone(&self) -> Self {
        Self {
            entry: self.entry.clone(),
        }
    }
}

impl<R: Resource> WeakResourceRef<R> {
    /// Attempts to acquire a strong lease on the referenced generation.
    ///
    /// Returns `None` if the generation was reclaimed, is cancelling, or has
    /// already finished.
    pub fn upgrade(&self) -> Option<ResourceRef<R>> {
        let entry = self.entry.upgrade()?;
        entry.control.acquire().then_some(ResourceRef { entry })
    }
}
