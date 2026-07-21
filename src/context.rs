use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    AcquireError, CanonicalPlacement, Placement, Resource, ResourceOutcome, ResourceRef,
    domain::{Domain, Registry, RegistrySlot, RegistrySlotState, TaskFinalizer},
    lifecycle::Control,
    reference::Entry,
};

#[derive(Clone)]
pub struct ResourceContext {
    domain: Arc<Domain>,
    cancellation: CancellationToken,
}

pub enum ResourceStatus<R: Resource> {
    Absent,
    Starting,
    Active(ResourceRef<R>),
}

impl ResourceContext {
    pub(crate) fn for_resource(domain: Arc<Domain>, cancellation: CancellationToken) -> Self {
        Self {
            domain,
            cancellation,
        }
    }

    pub async fn cancelled(&self) {
        tokio::select! {
            _ = self.cancellation.cancelled() => {},
            _ = self.domain.shutdown.cancelled() => {},
        }
    }

    pub async fn compute<F, T>(&self, work: F) -> Result<T, tokio::task::JoinError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(work).await
    }

    pub fn spawn<R: Resource>(&self, input: R::Input) -> Result<ResourceRef<R>, AcquireError> {
        if R::Placement::CANONICAL {
            self.acquire_canonical::<R>(input, true)
        } else {
            self.create_and_start::<R>(input, None)
        }
    }

    pub fn get<R: Resource>(
        &self,
        key: &<R::Placement as Placement<R>>::Key,
    ) -> Option<ResourceRef<R>>
    where
        R::Placement: CanonicalPlacement<R>,
    {
        let registry = self.domain.registry::<R>();
        let slot = registry.entries.lock().unwrap().get(key)?.clone();
        let state = slot.state.lock().unwrap();
        let RegistrySlotState::Active { entry, .. } = &*state else {
            return None;
        };
        let entry = entry.upgrade()?;
        if self.domain.try_acquire(&entry) {
            Some(ResourceRef { entry })
        } else {
            None
        }
    }

    pub fn status<R: Resource>(
        &self,
        key: &<R::Placement as Placement<R>>::Key,
    ) -> ResourceStatus<R>
    where
        R::Placement: CanonicalPlacement<R>,
    {
        let registry = self.domain.registry::<R>();
        let Some(slot) = registry.entries.lock().unwrap().get(key).cloned() else {
            return ResourceStatus::Absent;
        };
        let state = slot.state.lock().unwrap();
        match &*state {
            RegistrySlotState::Vacant => ResourceStatus::Absent,
            RegistrySlotState::Starting { .. } => ResourceStatus::Starting,
            RegistrySlotState::Active { entry, .. } => entry
                .upgrade()
                .filter(|entry| self.domain.try_acquire(entry))
                .map_or(ResourceStatus::Absent, |entry| {
                    ResourceStatus::Active(ResourceRef { entry })
                }),
        }
    }

    pub fn get_or_spawn<R: Resource>(&self, input: R::Input) -> Result<ResourceRef<R>, AcquireError>
    where
        R::Placement: CanonicalPlacement<R>,
    {
        self.acquire_canonical::<R>(input, false)
    }

    pub fn all<R: Resource>(&self) -> Vec<ResourceRef<R>> {
        let registry = self.domain.registry::<R>();
        let entries = registry
            .live
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        entries
            .into_iter()
            .filter_map(|weak| {
                let entry = weak.upgrade()?;
                if self.domain.try_acquire(&entry) {
                    Some(ResourceRef { entry })
                } else {
                    None
                }
            })
            .collect()
    }

    fn acquire_canonical<R: Resource>(
        &self,
        input: R::Input,
        create_only: bool,
    ) -> Result<ResourceRef<R>, AcquireError> {
        if !self.domain.is_accepting() {
            return Err(AcquireError::ShuttingDown);
        }
        let key = R::Placement::placement_key(&input);
        let registry = self.domain.registry::<R>();
        let (slot, mut owner) = {
            let mut entries = registry.entries.lock().unwrap();
            if let Some(slot) = entries.get(&key) {
                (slot.clone(), false)
            } else {
                let generation = 1;
                let slot = Arc::new(RegistrySlot {
                    state: std::sync::Mutex::new(RegistrySlotState::Starting { generation }),
                    changed: std::sync::Condvar::new(),
                });
                entries.insert(key.clone(), slot.clone());
                (slot, true)
            }
        };

        let mut state = slot.state.lock().unwrap();
        while !owner {
            match &*state {
                RegistrySlotState::Starting { .. } => state = slot.changed.wait(state).unwrap(),
                RegistrySlotState::Vacant => {
                    if !self.domain.is_accepting() {
                        return Err(AcquireError::ShuttingDown);
                    }
                    let generation = 1;
                    *state = RegistrySlotState::Starting { generation };
                    owner = true;
                }
                RegistrySlotState::Active { generation, entry } => {
                    if let Some(entry) = entry.upgrade()
                        && self.domain.try_acquire(&entry)
                    {
                        return if create_only {
                            entry.control.release();
                            Err(AcquireError::Occupied)
                        } else {
                            Ok(ResourceRef { entry })
                        };
                    }
                    if !self.domain.is_accepting() {
                        return Err(AcquireError::ShuttingDown);
                    }
                    let generation = *generation + 1;
                    *state = RegistrySlotState::Starting { generation };
                    owner = true;
                }
            }
        }
        let generation = match &*state {
            RegistrySlotState::Starting { generation } => *generation,
            _ => unreachable!(),
        };
        drop(state);
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
            Ok((resource, start)) => {
                let mut state = slot.state.lock().unwrap();
                *state = RegistrySlotState::Active {
                    generation,
                    entry: Arc::downgrade(&resource.entry),
                };
                slot.changed.notify_all();
                let _ = start.send(());
                Ok(resource)
            }
            Err(error) => {
                *slot.state.lock().unwrap() = RegistrySlotState::Vacant;
                Self::remove_slot(&registry, &key, &slot, Some(generation));
                slot.changed.notify_all();
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
            && generation.is_none_or(|expected| match &*slot.state.lock().unwrap() {
                RegistrySlotState::Vacant => true,
                RegistrySlotState::Starting { generation }
                | RegistrySlotState::Active { generation, .. } => *generation == expected,
            })
        {
            entries.remove(key);
        }
    }

    fn create_and_start<R: Resource>(
        &self,
        input: R::Input,
        on_finish: Option<Box<dyn FnOnce() + Send + 'static>>,
    ) -> Result<ResourceRef<R>, AcquireError> {
        let (resource, start) = self.create::<R>(input, on_finish)?;
        let _ = start.send(());
        Ok(resource)
    }

    fn create<R: Resource>(
        &self,
        input: R::Input,
        on_finish: Option<Box<dyn FnOnce() + Send + 'static>>,
    ) -> Result<(ResourceRef<R>, tokio::sync::oneshot::Sender<()>), AcquireError> {
        if !self.domain.is_accepting() {
            return Err(AcquireError::ShuttingDown);
        }
        let spec = catch_unwind(AssertUnwindSafe(|| R::build(input)))
            .map_err(|_| AcquireError::ConstructionPanicked)?;
        let control = Arc::new(Control::new());
        let (finished_tx, finished) = watch::channel(None);
        let entry = Arc::new(Entry {
            interface: spec.interface,
            control: control.clone(),
            finished,
            domain: Arc::downgrade(&self.domain),
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
        self.domain
            .registry::<R>()
            .live
            .lock()
            .unwrap()
            .insert(task_id, Arc::downgrade(&entry));
        let domain = self.domain.clone();
        tokio::spawn(async move {
            let _finalizer = TaskFinalizer::new(domain.clone(), task_id);
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
            let _ = catch_unwind(AssertUnwindSafe(|| {
                if let Some(cleanup) = on_finish {
                    cleanup();
                }
            }));
            let _ = finished_tx.send(Some(outcome));
            domain.registry::<R>().live.lock().unwrap().remove(&task_id);
        });
        Ok((ResourceRef { entry }, start_tx))
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

    use crate::{Keyed, Keyer, ResourceCompletion, ResourceSpec, Singleton, Unique};

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
        type Placement = Unique;
        fn build((): ()) -> ResourceSpec<Self, Self::Error> {
            ResourceSpec::new(Self, |_| async move {
                std::future::pending::<()>().await;
                Ok(())
            })
        }
    }

    struct FastResource;
    impl Resource for FastResource {
        type Input = ();
        type Error = Infallible;
        type Placement = Unique;
        fn build((): ()) -> ResourceSpec<Self, Self::Error> {
            ResourceSpec::new(Self, |_| async move { Ok(()) })
        }
    }

    struct StartingResource;
    impl Resource for StartingResource {
        type Input = Arc<std::sync::Barrier>;
        type Error = Infallible;
        type Placement = Singleton;
        fn build(barrier: Self::Input) -> ResourceSpec<Self, Self::Error> {
            barrier.wait();
            barrier.wait();
            ResourceSpec::new(Self, |cx| async move {
                cx.cancelled().await;
                Ok(())
            })
        }
    }

    struct ConstructionPanic;
    impl Resource for ConstructionPanic {
        type Input = ();
        type Error = Infallible;
        type Placement = Singleton;
        fn build((): ()) -> ResourceSpec<Self, Self::Error> {
            panic!("construction boom")
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

    #[tokio::test]
    async fn strong_references_clone_after_completion_and_during_shutdown() {
        let (domain, cx) = context();
        let finished = cx.spawn::<FastResource>(()).unwrap();
        assert!(matches!(
            finished.finished().await,
            ResourceOutcome::Completed
        ));
        let clone = finished.clone();
        drop(finished);
        drop(clone);

        let cancelling = cx.spawn::<StuckResource>(()).unwrap();
        assert_eq!(cancelling.entry.control.leases(), 1);
        domain.cancel();
        let clone = cancelling.clone();
        assert_eq!(cancelling.entry.control.leases(), 2);
        assert!(cancelling.downgrade().upgrade().is_none());
        assert_eq!(cancelling.entry.control.leases(), 2);
        drop((cancelling, clone));
        domain.terminate().await;
    }

    #[tokio::test]
    async fn all_includes_unique_generations_without_retaining_them() {
        let (domain, cx) = context();
        let first = cx.spawn::<StuckResource>(()).unwrap();
        let second = cx.spawn::<StuckResource>(()).unwrap();
        assert_eq!(cx.all::<StuckResource>().len(), 2);
        drop((first, second));
        domain.terminate().await;
        assert!(cx.all::<StuckResource>().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_reports_explicit_starting_state() {
        let (_, cx) = context();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let spawning = {
            let cx = cx.clone();
            let barrier = barrier.clone();
            tokio::task::spawn_blocking(move || {
                cx.get_or_spawn::<StartingResource>(barrier).unwrap()
            })
        };
        barrier.wait();
        assert!(matches!(
            cx.status::<StartingResource>(&()),
            ResourceStatus::Starting
        ));
        barrier.wait();
        let resource = spawning.await.unwrap();
        assert!(matches!(
            cx.status::<StartingResource>(&()),
            ResourceStatus::Active(_)
        ));
        drop(resource);
    }

    #[test]
    fn construction_panic_restores_absent_slot() {
        let (_, cx) = context();
        assert!(matches!(
            cx.get_or_spawn::<ConstructionPanic>(()),
            Err(AcquireError::ConstructionPanicked)
        ));
        assert!(matches!(
            cx.status::<ConstructionPanic>(&()),
            ResourceStatus::Absent
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canonical_construction_racing_shutdown_cannot_activate() {
        let (domain, cx) = context();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let spawning = {
            let cx = cx.clone();
            let barrier = barrier.clone();
            tokio::task::spawn_blocking(move || cx.get_or_spawn::<StartingResource>(barrier))
        };
        barrier.wait();
        domain.cancel();
        barrier.wait();
        assert!(matches!(
            spawning.await.unwrap(),
            Err(AcquireError::ShuttingDown)
        ));
        assert!(matches!(
            cx.status::<StartingResource>(&()),
            ResourceStatus::Absent
        ));
        domain.shutdown().await;
    }

    #[tokio::test]
    async fn cleanup_panic_still_publishes_and_quiesces() {
        let (domain, cx) = context();
        let resource = cx
            .create_and_start::<FastResource>((), Some(Box::new(|| panic!("cleanup boom"))))
            .unwrap();
        assert!(matches!(
            resource.finished().await,
            ResourceOutcome::Completed
        ));
        domain.shutdown().await;
        assert_eq!(domain.active_tasks(), 0);
    }
}
