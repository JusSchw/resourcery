use std::{
    ops::Deref,
    sync::{Arc, Weak},
};

use tokio::sync::watch;

use crate::{Resource, domain::Domain, lifecycle::Control};

#[derive(Debug)]
pub enum ResourceOutcome<E> {
    Completed,
    Failed(Arc<E>),
    Panicked(Arc<str>),
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
    pub fn downgrade(&self) -> WeakResourceRef<R> {
        WeakResourceRef {
            entry: Arc::downgrade(&self.entry),
            domain: self.entry.domain.clone(),
        }
    }

    pub fn completion(&self) -> ResourceCompletion<R::Error> {
        ResourceCompletion {
            receiver: self.entry.finished.clone(),
        }
    }

    pub async fn finished(&self) -> ResourceOutcome<R::Error> {
        self.completion().wait().await
    }
}

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
