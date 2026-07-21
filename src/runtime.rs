use std::sync::Arc;

use crate::{Resource, ResourceContext, ResourceOutcome, RunError, domain::Domain};

/// A host for one shared resource domain.
///
/// Clones share the same identity space and lifecycle boundary. Distinct
/// runtimes contain independent generations even for identical types and keys.
#[derive(Clone)]
pub struct ResourceRuntime {
    domain: Arc<Domain>,
}

impl Default for ResourceRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceRuntime {
    /// Creates an empty, accepting resource domain.
    pub fn new() -> Self {
        Self {
            domain: Arc::new(Domain::new()),
        }
    }

    /// Establishes a new root generation and waits for its outcome.
    ///
    /// The root is created with [`ResourceContext::spawn`](crate::ResourceContext::spawn).
    /// Its strong reference is retained until the task finishes, so a root that
    /// waits only for lease-driven cancellation will run until the domain is
    /// cancelled from another runtime clone.
    pub async fn run<R: Resource>(&self, input: R::Input) -> Result<(), RunError<R::Error>> {
        let cx =
            ResourceContext::for_resource(self.domain.clone(), self.domain.shutdown.child_token());
        let root = cx.spawn::<R>(input).map_err(RunError::Acquire)?;
        match root.finished().await {
            ResourceOutcome::Completed => Ok(()),
            ResourceOutcome::Failed(error) => Err(RunError::Resource(error)),
            ResourceOutcome::Panicked(message) => Err(RunError::Panicked(message)),
            ResourceOutcome::Aborted => Err(RunError::Aborted),
        }
    }

    /// Begins cooperative domain shutdown without waiting for tasks to finish.
    ///
    /// This operation is idempotent. It rejects future acquisition, signals all
    /// managed tasks, and leaves existing strong references usable.
    pub fn cancel(&self) {
        self.domain.cancel();
    }

    /// Begins cooperative shutdown and waits until all managed tasks terminate.
    ///
    /// A task that ignores cancellation can make this future wait indefinitely;
    /// use [`terminate`](Self::terminate) when forced abortion is required.
    pub async fn shutdown(&self) {
        self.domain.shutdown().await;
    }

    /// Begins shutdown, aborts remaining tasks, and waits for quiescence.
    ///
    /// Aborted generations publish [`ResourceOutcome::Aborted`](crate::ResourceOutcome::Aborted).
    pub async fn terminate(&self) {
        self.domain.terminate().await;
    }
}
