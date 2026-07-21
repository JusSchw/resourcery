use std::sync::Arc;

use crate::{Resource, ResourceContext, ResourceOutcome, RunError, domain::Domain};

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
    pub fn new() -> Self {
        Self {
            domain: Arc::new(Domain::new()),
        }
    }

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

    pub fn cancel(&self) {
        self.domain.cancel();
    }

    pub async fn shutdown(&self) {
        self.domain.shutdown().await;
    }

    pub async fn terminate(&self) {
        self.domain.terminate().await;
    }
}
