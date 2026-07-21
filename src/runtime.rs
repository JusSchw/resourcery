use std::sync::Arc;

use crate::{Resource, ResourceContext, RunError, domain::Domain};

/// Owns and executes one isolated resource domain.
pub struct ResourceRuntime {
    domain: Arc<Domain>,
}

impl Default for ResourceRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceRuntime {
    /// Creates a new, isolated resource domain.
    ///
    /// The runtime uses the currently active Tokio executor when resources are
    /// spawned. Constructing it does not start any tasks.
    pub fn new() -> Self {
        Self {
            domain: Arc::new(Domain::new()),
        }
    }

    /// Creates a root resource and waits for its managed task to finish.
    ///
    /// The runtime retains the root reference for the duration of this call.
    /// Dependencies retained by the root are released when its task returns.
    /// Separate calls use the same domain and therefore share canonical
    /// resources unless shutdown has begun.
    pub async fn run<R: Resource>(&self, input: R::Input) -> Result<(), RunError<R::Error>> {
        let cx =
            ResourceContext::for_resource(self.domain.clone(), self.domain.shutdown.child_token());
        let root = cx.spawn::<R>(input).map_err(RunError::Acquire)?;
        root.finished().await.map_err(RunError::Resource)
    }

    /// Requests cooperative cancellation of every tracked generation.
    ///
    /// Shutdown is permanent for this runtime: subsequent acquisitions fail or
    /// return no resource. This method does not wait for tasks to finish.
    pub fn shutdown(&self) {
        self.domain.shutdown();
    }
}
