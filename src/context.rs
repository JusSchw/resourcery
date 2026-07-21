use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Weak},
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    AcquireError, Placement, Resource, ResourceOutcome, ResourceRef,
    domain::{Domain, Registry, RegistrySlot},
    lifecycle::Control,
    reference::Entry,
};

/// Capabilities made available to a managed resource task.
///
/// Contexts and references are ordinary owned domain handles. They have no
/// cosmetic lifetime parameter: any restrictions on retaining them are the
/// normal `Send`, `Sync`, and `'static` restrictions enforced by Rust.
#[derive(Clone)]
pub struct ResourceContext {
    domain: Arc<Domain>,
    cancellation: CancellationToken,
}

/// Observable acquisition state for one canonical identity.
pub enum ResourceStatus<R: Resource> {
    /// No active or establishing generation has this identity.
    Absent,
    /// A caller is currently constructing this identity.
    Starting,
    /// The canonical generation is active; this variant contains a new lease.
    Active(ResourceRef<R>),
}

impl ResourceContext {
    pub(crate) fn for_resource(domain: Arc<Domain>, cancellation: CancellationToken) -> Self {
        Self {
            domain,
            cancellation,
        }
    }

    /// Waits until this generation or its domain is cancelled.
    pub async fn cancelled(&self) {
        tokio::select! {
            _ = self.cancellation.cancelled() => {},
            _ = self.domain.shutdown.cancelled() => {},
        }
    }

    /// Executes bounded synchronous work on Tokio's blocking pool.
    pub async fn compute<F, T>(&self, work: F) -> Result<T, tokio::task::JoinError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(work).await
    }

    /// Creates a generation, failing if a canonical identity is occupied.
    pub fn spawn<R: Resource>(&self, input: R::Input) -> Result<ResourceRef<R>, AcquireError> {
        if !self.domain.is_running() {
            return Err(AcquireError::ShuttingDown);
        }
        if R::Placement::CANONICAL {
            self.acquire_canonical::<R>(input, true)
        } else {
            self.create::<R>(input, None)
        }
    }

    /// Retrieves an active canonical generation without creating it.
    pub fn get<R: Resource>(
        &self,
        key: &<R::Placement as Placement<R>>::Key,
    ) -> Option<ResourceRef<R>> {
        if !R::Placement::CANONICAL || !self.domain.is_running() {
            return None;
        }
        let registry = self.domain.registry::<R>();
        let slot = registry.entries.lock().unwrap().get(key)?.clone();
        let entry = slot.entry.lock().unwrap().upgrade()?;
        entry.control.acquire().then_some(ResourceRef { entry })
    }

    /// Inspects a canonical identity without waiting for ongoing construction.
    pub fn status<R: Resource>(
        &self,
        key: &<R::Placement as Placement<R>>::Key,
    ) -> ResourceStatus<R> {
        if !R::Placement::CANONICAL || !self.domain.is_running() {
            return ResourceStatus::Absent;
        }
        let registry = self.domain.registry::<R>();
        let Some(slot) = registry.entries.lock().unwrap().get(key).cloned() else {
            return ResourceStatus::Absent;
        };
        let Ok(current) = slot.entry.try_lock() else {
            return ResourceStatus::Starting;
        };
        let Some(entry) = current.upgrade() else {
            return ResourceStatus::Absent;
        };
        if entry.control.acquire() {
            ResourceStatus::Active(ResourceRef { entry })
        } else {
            ResourceStatus::Absent
        }
    }

    /// Retrieves an active canonical generation or atomically creates it.
    pub fn get_or_spawn<R: Resource>(
        &self,
        input: R::Input,
    ) -> Result<ResourceRef<R>, AcquireError> {
        if !R::Placement::CANONICAL {
            return self.create::<R>(input, None);
        }
        self.acquire_canonical::<R>(input, false)
    }

    /// Returns leases for all currently active canonical generations of `R`.
    pub fn all<R: Resource>(&self) -> Vec<ResourceRef<R>> {
        if !R::Placement::CANONICAL || !self.domain.is_running() {
            return Vec::new();
        }
        let registry = self.domain.registry::<R>();
        let slots = registry
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        slots
            .into_iter()
            .filter_map(|slot| {
                let entry = slot.entry.lock().unwrap().upgrade()?;
                entry.control.acquire().then_some(ResourceRef { entry })
            })
            .collect()
    }

    fn acquire_canonical<R: Resource>(
        &self,
        input: R::Input,
        create_only: bool,
    ) -> Result<ResourceRef<R>, AcquireError> {
        if !self.domain.is_running() {
            return Err(AcquireError::ShuttingDown);
        }
        let key = R::Placement::key(&input);
        let registry = self.domain.registry::<R>();
        let slot = {
            let mut entries = registry.entries.lock().unwrap();
            entries
                .entry(key.clone())
                .or_insert_with(|| {
                    Arc::new(RegistrySlot {
                        entry: std::sync::Mutex::new(Weak::new()),
                        generation: std::sync::atomic::AtomicU64::new(0),
                    })
                })
                .clone()
        };

        // The per-identity lock is the explicit Starting state. It serializes
        // construction of this identity without blocking unrelated keys.
        let mut current = slot.entry.lock().unwrap();
        if let Some(entry) = current.upgrade()
            && entry.control.acquire()
        {
            return if create_only {
                entry.control.release();
                Err(AcquireError::Occupied)
            } else {
                Ok(ResourceRef { entry })
            };
        }
        if !self.domain.is_running() {
            Self::remove_slot(&registry, &key, &slot, None);
            return Err(AcquireError::ShuttingDown);
        }
        let generation = slot
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let cleanup_registry = registry.clone();
        let cleanup_key = key.clone();
        let cleanup_slot = slot.clone();
        let resource = self.create::<R>(
            input,
            Some(Box::new(move || {
                Self::remove_slot(
                    &cleanup_registry,
                    &cleanup_key,
                    &cleanup_slot,
                    Some(generation),
                );
            })),
        );
        match resource {
            Ok(resource) => {
                *current = Arc::downgrade(&resource.entry);
                Ok(resource)
            }
            Err(error) => {
                drop(current);
                Self::remove_slot(&registry, &key, &slot, Some(generation));
                Err(error)
            }
        }
    }

    fn remove_slot<R: Resource>(
        registry: &Registry<R>,
        key: &<R::Placement as Placement<R>>::Key,
        slot: &Arc<RegistrySlot<R>>,
        generation: Option<u64>,
    ) {
        let mut entries = registry.entries.lock().unwrap();
        if entries
            .get(key)
            .is_some_and(|present| Arc::ptr_eq(present, slot))
            && generation.is_none_or(|expected| {
                slot.generation.load(std::sync::atomic::Ordering::SeqCst) == expected
            })
        {
            entries.remove(key);
        }
    }

    fn create<R: Resource>(
        &self,
        input: R::Input,
        on_finish: Option<Box<dyn FnOnce() + Send + 'static>>,
    ) -> Result<ResourceRef<R>, AcquireError> {
        let spec = catch_unwind(AssertUnwindSafe(|| R::build(input)))
            .map_err(|_| AcquireError::ConstructionPanicked)?;
        let control = Arc::new(Control::new());
        let (finished_tx, finished) = watch::channel(None);
        let entry = Arc::new(Entry {
            interface: spec.interface,
            control: control.clone(),
            finished,
        });
        let cx = ResourceContext::for_resource(self.domain.clone(), control.cancellation.clone());
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = start_rx.await;
            (spec.task)(cx).await
        });
        let Some(task_id) = self.domain.register(control.clone(), task.abort_handle()) else {
            task.abort();
            control.cancel();
            return Err(AcquireError::ShuttingDown);
        };
        let _ = start_tx.send(());
        let domain = self.domain.clone();
        tokio::spawn(async move {
            let outcome = match task.await {
                Ok(Ok(())) => ResourceOutcome::Completed,
                Ok(Err(error)) => ResourceOutcome::Failed(Arc::new(error)),
                Err(error) if error.is_panic() => {
                    let payload = error.into_panic();
                    let message = payload
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("resource task panicked");
                    ResourceOutcome::Panicked(Arc::from(message))
                }
                Err(_) => ResourceOutcome::Aborted,
            };
            control.finish();
            if let Some(cleanup) = on_finish {
                cleanup();
            }
            let _ = finished_tx.send(Some(outcome));
            domain.task_finished(task_id);
        });
        Ok(ResourceRef { entry })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::time::{Duration, timeout};

    use crate::{Keyed, Keyer, ResourceCompletion, ResourceSpec, Singleton};

    static BUILDS: AtomicUsize = AtomicUsize::new(0);

    struct TestResource(usize);
    struct TestInput {
        key: usize,
        cancellations: Arc<AtomicUsize>,
    }
    struct ByKey;

    impl Keyer<TestResource> for ByKey {
        type Key = usize;
        fn key(input: &TestInput) -> usize {
            input.key
        }
    }

    impl Resource for TestResource {
        type Input = TestInput;
        type Error = Infallible;
        type Placement = Keyed<ByKey>;

        fn build(input: Self::Input) -> ResourceSpec<Self, Self::Error> {
            let generation = BUILDS.fetch_add(1, Ordering::SeqCst);
            ResourceSpec::new(Self(generation), move |cx| async move {
                cx.cancelled().await;
                input.cancellations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct PanicResource;
    impl Resource for PanicResource {
        type Input = ();
        type Error = Infallible;
        type Placement = Singleton;
        fn build((): ()) -> ResourceSpec<Self, Self::Error> {
            ResourceSpec::new(Self, |_| async move { panic!("managed boom") })
        }
    }

    struct StuckResource;
    impl Resource for StuckResource {
        type Input = ();
        type Error = Infallible;
        type Placement = Singleton;
        fn build((): ()) -> ResourceSpec<Self, Self::Error> {
            ResourceSpec::new(Self, |_| async move {
                std::future::pending::<()>().await;
                Ok(())
            })
        }
    }

    fn context() -> (Arc<Domain>, ResourceContext) {
        let domain = Arc::new(Domain::new());
        let cx = ResourceContext::for_resource(domain.clone(), domain.shutdown.child_token());
        (domain, cx)
    }

    #[tokio::test]
    async fn completion_does_not_hold_a_lease_and_is_multicast() {
        let (_, cx) = context();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let resource = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: 1,
                cancellations: cancellations.clone(),
            })
            .unwrap();
        let first: ResourceCompletion<Infallible> = resource.completion();
        let second = first.clone();
        drop(resource);

        assert!(matches!(
            timeout(Duration::from_secs(1), first.wait()).await.unwrap(),
            ResourceOutcome::Completed
        ));
        assert!(matches!(second.wait().await, ResourceOutcome::Completed));
        assert_eq!(cancellations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn panic_is_a_stable_terminal_outcome() {
        let (_, cx) = context();
        let resource = cx.spawn::<PanicResource>(()).unwrap();
        let one = resource.completion();
        let two = one.clone();
        assert!(
            matches!(one.wait().await, ResourceOutcome::Panicked(message) if message.contains("managed boom"))
        );
        assert!(matches!(two.wait().await, ResourceOutcome::Panicked(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_canonical_acquisition_builds_once() {
        BUILDS.store(0, Ordering::SeqCst);
        let (_, cx) = context();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let mut joins = Vec::new();
        for _ in 0..16 {
            let cx = cx.clone();
            let cancellations = cancellations.clone();
            joins.push(tokio::task::spawn_blocking(move || {
                cx.get_or_spawn::<TestResource>(TestInput {
                    key: 7,
                    cancellations,
                })
                .unwrap()
            }));
        }
        let mut resources = Vec::new();
        for join in joins {
            resources.push(join.await.unwrap());
        }
        assert!(
            resources
                .iter()
                .all(|resource| resource.0 == resources[0].0)
        );
        drop(resources);
    }

    #[tokio::test]
    async fn terminal_generation_is_removed_and_old_cleanup_is_generation_safe() {
        let (domain, cx) = context();
        let old_cancel = Arc::new(AtomicUsize::new(0));
        let old = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: 9,
                cancellations: old_cancel,
            })
            .unwrap();
        let old_done = old.completion();
        drop(old);
        let replacement = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: 9,
                cancellations: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();
        old_done.wait().await;
        assert_eq!(cx.get::<TestResource>(&9).unwrap().0, replacement.0);
        drop(replacement);
        domain.shutdown().await;
        assert_eq!(
            domain
                .registry::<TestResource>()
                .entries
                .lock()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(domain.active_tasks(), 0);
    }

    #[tokio::test]
    async fn repeated_create_and_drop_does_not_grow_registry_or_supervisor() {
        let (domain, cx) = context();
        for generation in 0..100 {
            let resource = cx
                .get_or_spawn::<TestResource>(TestInput {
                    key: generation,
                    cancellations: Arc::new(AtomicUsize::new(0)),
                })
                .unwrap();
            let completion = resource.completion();
            drop(resource);
            completion.wait().await;
        }
        assert!(
            domain
                .registry::<TestResource>()
                .entries
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(domain.active_tasks(), 0);
    }

    #[tokio::test]
    async fn shutdown_is_awaitable_idempotent_and_rejects_acquisition() {
        let (domain, cx) = context();
        let resource = cx
            .get_or_spawn::<TestResource>(TestInput {
                key: 11,
                cancellations: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();
        domain.shutdown().await;
        assert!(matches!(
            resource.finished().await,
            ResourceOutcome::Completed
        ));
        assert!(matches!(
            cx.get_or_spawn::<TestResource>(TestInput {
                key: 12,
                cancellations: Arc::new(AtomicUsize::new(0)),
            }),
            Err(AcquireError::ShuttingDown)
        ));
        domain.shutdown().await;
    }

    #[tokio::test]
    async fn forced_termination_publishes_aborted_and_quiesces() {
        let (domain, cx) = context();
        let resource = cx.spawn::<StuckResource>(()).unwrap();
        let completion = resource.completion();
        domain.terminate().await;
        assert!(matches!(completion.wait().await, ResourceOutcome::Aborted));
        assert_eq!(domain.active_tasks(), 0);
    }
}
