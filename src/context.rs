use std::{
    any::TypeId,
    marker::PhantomData,
    sync::{Arc, Weak},
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    AcquireError, Placement, Resource, ResourceRef,
    domain::{Domain, Registry},
    lifecycle::Control,
    reference::Entry,
};

/// Capabilities made available to a managed resource task.
#[derive(Clone)]
pub struct ResourceContext<'context> {
    domain: Arc<Domain>,
    cancellation: CancellationToken,
    context: PhantomData<&'context ()>,
}

impl ResourceContext<'static> {
    pub(crate) fn for_resource(domain: Arc<Domain>, cancellation: CancellationToken) -> Self {
        Self {
            domain,
            cancellation,
            context: PhantomData,
        }
    }
}

impl<'context> ResourceContext<'context> {
    /// Waits until this resource or its entire runtime domain is cancelled.
    ///
    /// Cancellation is cooperative. Returning from this future acknowledges
    /// the request but does not impose a deadline or abort the task.
    pub async fn cancelled(&self) {
        tokio::select! {
            _ = self.cancellation.cancelled() => {},
            _ = self.domain.shutdown.cancelled() => {},
        }
    }

    /// Executes synchronous work on Tokio's blocking thread pool.
    ///
    /// Use this for bounded CPU-heavy or blocking work that produces one value.
    /// The operation has no resource identity and is not added to the registry.
    /// A panic or cancellation of the blocking task is reported as a Tokio
    /// [`JoinError`](tokio::task::JoinError).
    pub async fn compute<F, T>(&self, work: F) -> Result<T, tokio::task::JoinError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(work).await
    }

    /// Creates a new resource generation.
    ///
    /// [`Unique`](crate::Unique) resources are always created. For
    /// [`Singleton`](crate::Singleton) and [`Keyed`](crate::Keyed) resources,
    /// this is a create-only operation and returns [`AcquireError::Occupied`]
    /// when the identity already has an active generation.
    pub fn spawn<R: Resource>(
        &self,
        input: R::Input,
    ) -> Result<ResourceRef<'context, R>, AcquireError> {
        if self.domain.shutdown.is_cancelled() {
            return Err(AcquireError::ShuttingDown);
        }
        if R::Placement::CANONICAL {
            self.spawn_canonical::<R>(input)
        } else {
            Ok(self.create::<R>(input))
        }
    }

    /// Retrieves an active canonical generation without creating it.
    ///
    /// Returns `None` for unique placement, an unknown identity, a generation
    /// that is cancelling or finished, or a domain that is shutting down.
    pub fn get<R: Resource>(
        &self,
        key: &<R::Placement as Placement<R>>::Key,
    ) -> Option<ResourceRef<'context, R>> {
        if !R::Placement::CANONICAL || self.domain.shutdown.is_cancelled() {
            return None;
        }
        let registries = self.domain.registries.lock().unwrap();
        let registry = registries
            .get(&TypeId::of::<R>())?
            .downcast_ref::<Registry<R>>()?;
        let entry = registry.entries.get(key)?.upgrade()?;
        entry.control.acquire().then_some(ResourceRef {
            entry,
            context: PhantomData,
        })
    }

    /// Retrieves an active canonical generation or atomically creates it.
    ///
    /// For unique placement this always creates. For keyed placement, fields of
    /// `input` excluded from the key are used only if creation wins; they never
    /// reconfigure an existing generation.
    pub fn get_or_spawn<R: Resource>(
        &self,
        input: R::Input,
    ) -> Result<ResourceRef<'context, R>, AcquireError> {
        if !R::Placement::CANONICAL {
            return Ok(self.create::<R>(input));
        }
        if self.domain.shutdown.is_cancelled() {
            return Err(AcquireError::ShuttingDown);
        }
        let key = R::Placement::key(&input);
        let mut registries = self.domain.registries.lock().unwrap();
        let registry = registries
            .entry(TypeId::of::<R>())
            .or_insert_with(|| Box::new(Registry::<R>::default()))
            .downcast_mut::<Registry<R>>()
            .unwrap();
        if let Some(entry) = registry.entries.get(&key).and_then(Weak::upgrade)
            && entry.control.acquire()
        {
            return Ok(ResourceRef {
                entry,
                context: PhantomData,
            });
        }
        let resource = self.create::<R>(input);
        registry
            .entries
            .insert(key, Arc::downgrade(&resource.entry));
        Ok(resource)
    }

    /// Returns strong references to all active canonical resources of type `R`.
    ///
    /// Unique resources are not discoverable and therefore produce an empty
    /// vector. Every returned reference keeps its generation alive.
    pub fn all<R: Resource>(&self) -> Vec<ResourceRef<'context, R>> {
        if !R::Placement::CANONICAL || self.domain.shutdown.is_cancelled() {
            return Vec::new();
        }
        let mut registries = self.domain.registries.lock().unwrap();
        let Some(registry) = registries
            .get_mut(&TypeId::of::<R>())
            .and_then(|registry| registry.downcast_mut::<Registry<R>>())
        else {
            return Vec::new();
        };
        let mut resources = Vec::new();
        registry.entries.retain(|_, weak| {
            let Some(entry) = weak.upgrade() else {
                return false;
            };
            if entry.control.acquire() {
                resources.push(ResourceRef {
                    entry,
                    context: PhantomData,
                });
                true
            } else {
                false
            }
        });
        resources
    }

    fn spawn_canonical<R: Resource>(
        &self,
        input: R::Input,
    ) -> Result<ResourceRef<'context, R>, AcquireError> {
        let key = R::Placement::key(&input);
        let mut registries = self.domain.registries.lock().unwrap();
        let registry = registries
            .entry(TypeId::of::<R>())
            .or_insert_with(|| Box::new(Registry::<R>::default()))
            .downcast_mut::<Registry<R>>()
            .unwrap();
        if registry
            .entries
            .get(&key)
            .and_then(Weak::upgrade)
            .is_some_and(|entry| entry.control.is_active())
        {
            return Err(AcquireError::Occupied);
        }
        let resource = self.create::<R>(input);
        registry
            .entries
            .insert(key, Arc::downgrade(&resource.entry));
        Ok(resource)
    }

    fn create<R: Resource>(&self, input: R::Input) -> ResourceRef<'context, R> {
        let spec = R::build(input);
        let control = Arc::new(Control::new());
        let (finished_tx, finished) = watch::channel(None);
        let entry = Arc::new(Entry {
            interface: spec.interface,
            control: control.clone(),
            finished,
        });
        self.domain.track(&control);
        let cx = ResourceContext::for_resource(self.domain.clone(), control.cancellation.clone());
        tokio::spawn(async move {
            let result = (spec.task)(cx).await.map_err(Arc::new);
            control.finish();
            let _ = finished_tx.send(Some(result));
        });
        ResourceRef {
            entry,
            context: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, timeout};

    use crate::{Keyed, Keyer, Never, ResourceSpec};

    static NEXT_GENERATION: AtomicUsize = AtomicUsize::new(1);

    struct TestResource {
        generation: usize,
    }

    struct TestInput {
        key: String,
        cancelled: Arc<AtomicUsize>,
    }

    struct ByName;

    impl Keyer<TestResource> for ByName {
        type Key = String;

        fn key(input: &TestInput) -> Self::Key {
            input.key.clone()
        }
    }

    impl Resource for TestResource {
        type Input = TestInput;
        type Error = Never;
        type Placement = Keyed<ByName>;

        fn build(input: Self::Input) -> ResourceSpec<Self, Self::Error> {
            let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
            ResourceSpec::new(Self { generation }, move |cx| async move {
                cx.cancelled().await;
                input.cancelled.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn context() -> ResourceContext<'static> {
        let domain = Arc::new(Domain::new());
        ResourceContext::for_resource(domain.clone(), domain.shutdown.child_token())
    }

    async fn wait_for_cancellation(counter: &AtomicUsize) {
        timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn keyed_resources_are_deduplicated_and_discoverable() {
        let cx = context();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let first = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: "shared".into(),
                cancelled: cancellations.clone(),
            })
            .unwrap();
        let second = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: "shared".into(),
                cancelled: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();

        assert_eq!(first.generation, second.generation);
        assert_eq!(
            cx.get::<TestResource>(&"shared".into()).unwrap().generation,
            first.generation
        );
        assert_eq!(cx.all::<TestResource>().len(), 1);

        drop(first);
        drop(second);
        wait_for_cancellation(&cancellations).await;
    }

    #[tokio::test]
    async fn final_release_allows_a_new_generation() {
        let cx = context();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let first = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: "service".into(),
                cancelled: cancellations.clone(),
            })
            .unwrap();
        let generation = first.generation;
        let weak = first.downgrade();
        drop(first);

        assert!(weak.upgrade().is_none());
        let replacement = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: "service".into(),
                cancelled: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();
        assert_ne!(replacement.generation, generation);
        wait_for_cancellation(&cancellations).await;
    }

    #[tokio::test]
    async fn create_only_spawn_reports_conflicts() {
        let cx = context();
        let resource = cx
            .spawn::<TestResource>(TestInput {
                key: "occupied".into(),
                cancelled: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();
        let conflict = cx.spawn::<TestResource>(TestInput {
            key: "occupied".into(),
            cancelled: Arc::new(AtomicUsize::new(0)),
        });

        assert!(matches!(conflict, Err(AcquireError::Occupied)));
        drop(resource);
    }
}
