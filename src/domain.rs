use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, Ordering},
    },
};

use dashmap::DashMap;
use event_listener::{Event, EventListener};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{Placement, Resource, lifecycle::Control, reference::Entry};

type ResourceKey<R> = <<R as Resource>::Placement as Placement<R>>::Key;

pub(crate) struct Registry<R: Resource> {
    entries: DashMap<ResourceKey<R>, Arc<RegistrySlot<R>>>,
    live: DashMap<u64, Weak<Entry<R>>>,
}

impl<R: Resource> Registry<R> {
    pub(crate) fn slot(&self, key: &ResourceKey<R>) -> Option<Arc<RegistrySlot<R>>> {
        self.entries.get(key).map(|slot| Arc::clone(slot.value()))
    }

    pub(crate) fn claim(
        &self,
        key: &ResourceKey<R>,
        domain: &Domain,
        create_only: bool,
    ) -> RegistryClaim<R> {
        self.claim_inner(key, domain, create_only, || {})
    }

    // Canonical ownership is linearized while the map entry is guarded. The
    // lock order is always map entry/shard first, then slot state. Returning a
    // listener created under both guards also closes the notification race.
    fn claim_inner(
        &self,
        key: &ResourceKey<R>,
        domain: &Domain,
        create_only: bool,
        after_entry_lock: impl FnOnce(),
    ) -> RegistryClaim<R> {
        use dashmap::mapref::entry::Entry;
        match self.entries.entry(key.clone()) {
            Entry::Occupied(entry) => {
                after_entry_lock();
                let slot = Arc::clone(entry.get());
                let mut state = slot.state.lock();
                match &*state {
                    RegistrySlotState::Starting { .. } => {
                        let listener = slot.changed.listen();
                        drop(state);
                        drop(entry);
                        RegistryClaim::Wait(listener)
                    }
                    RegistrySlotState::Active {
                        generation,
                        entry: weak,
                    } => {
                        if let Some(resource) = weak.upgrade()
                            && domain.try_acquire(&resource)
                        {
                            drop(state);
                            drop(entry);
                            if create_only {
                                RegistryClaim::Occupied
                            } else {
                                RegistryClaim::Active(resource)
                            }
                        } else if !domain.is_accepting() {
                            RegistryClaim::ShuttingDown
                        } else {
                            let generation = *generation + 1;
                            *state = RegistrySlotState::Starting { generation };
                            drop(state);
                            drop(entry);
                            RegistryClaim::Owner { slot, generation }
                        }
                    }
                }
            }
            Entry::Vacant(entry) => {
                after_entry_lock();
                if !domain.is_accepting() {
                    return RegistryClaim::ShuttingDown;
                }
                let slot = Arc::new(RegistrySlot::starting(1));
                entry.insert(slot.clone());
                RegistryClaim::Owner {
                    slot,
                    generation: 1,
                }
            }
        }
    }

    #[cfg(test)]
    fn claim_with_hook(
        &self,
        key: &ResourceKey<R>,
        domain: &Domain,
        after_entry_lock: impl FnOnce(),
    ) -> RegistryClaim<R> {
        self.claim_inner(key, domain, false, after_entry_lock)
    }

    pub(crate) fn remove_if_same(
        &self,
        key: &ResourceKey<R>,
        slot: &Arc<RegistrySlot<R>>,
        generation: u64,
    ) {
        // `remove_if` holds the map shard while `has_generation` locks the slot,
        // matching `claim_inner`'s ordering. Never call this while holding a
        // slot-state guard.
        self.entries.remove_if(key, |_, present| {
            Arc::ptr_eq(present, slot) && slot.has_generation(generation)
        });
    }

    pub(crate) fn insert_live(&self, id: u64, entry: Weak<Entry<R>>) {
        self.live.insert(id, entry);
    }

    pub(crate) fn remove_live(&self, id: u64) {
        self.live.remove(&id);
    }

    pub(crate) fn live_snapshot(&self) -> Vec<Weak<Entry<R>>> {
        self.live
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn entries_is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn entries_len(&self) -> usize {
        self.entries.len()
    }
}

pub(crate) struct RegistrySlot<R: Resource> {
    pub(crate) state: Mutex<RegistrySlotState<R>>,
    pub(crate) changed: Event,
}

impl<R: Resource> RegistrySlot<R> {
    pub(crate) fn starting(generation: u64) -> Self {
        Self {
            state: Mutex::new(RegistrySlotState::Starting { generation }),
            changed: Event::new(),
        }
    }

    fn has_generation(&self, expected: u64) -> bool {
        match &*self.state.lock() {
            RegistrySlotState::Starting { generation }
            | RegistrySlotState::Active { generation, .. } => *generation == expected,
        }
    }
}

pub(crate) enum RegistryClaim<R: Resource> {
    Active(Arc<Entry<R>>),
    Owner {
        slot: Arc<RegistrySlot<R>>,
        generation: u64,
    },
    Wait(EventListener),
    Occupied,
    ShuttingDown,
}

pub(crate) enum RegistrySlotState<R: Resource> {
    Starting {
        generation: u64,
    },
    Active {
        generation: u64,
        entry: Weak<Entry<R>>,
    },
}

impl<R: Resource> Default for Registry<R> {
    fn default() -> Self {
        Self {
            entries: DashMap::new(),
            live: DashMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum DomainPhase {
    Running,
    ShuttingDown,
    Terminated,
}

struct Supervisor {
    next_task: u64,
    tasks: HashMap<u64, ManagedTask>,
}
struct ManagedTask {
    control: Arc<Control>,
    abort: tokio::task::AbortHandle,
}

pub(crate) struct Domain {
    registries: DashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    phase: AtomicU8,
    supervisor: Mutex<Supervisor>,
    quiescent: Notify,
    pub(crate) shutdown: CancellationToken,
}

pub(crate) struct TaskFinalizer {
    domain: Arc<Domain>,
    task_id: u64,
}

impl TaskFinalizer {
    pub(crate) fn new(domain: Arc<Domain>, task_id: u64) -> Self {
        Self { domain, task_id }
    }
}

impl Drop for TaskFinalizer {
    fn drop(&mut self) {
        self.domain.task_finished(self.task_id);
    }
}

impl Domain {
    pub(crate) fn new() -> Self {
        Self {
            registries: DashMap::new(),
            phase: AtomicU8::new(DomainPhase::Running as u8),
            supervisor: Mutex::new(Supervisor {
                next_task: 0,
                tasks: HashMap::new(),
            }),
            quiescent: Notify::new(),
            shutdown: CancellationToken::new(),
        }
    }

    pub(crate) fn registry<R: Resource>(&self) -> Arc<Registry<R>> {
        let erased = self
            .registries
            .entry(TypeId::of::<R>())
            .or_insert_with(|| Arc::new(Registry::<R>::default()))
            .value()
            .clone();
        erased
            .downcast::<Registry<R>>()
            .expect("a resource type has exactly one registry type")
    }

    pub(crate) fn register(
        &self,
        control: Arc<Control>,
        abort: tokio::task::AbortHandle,
    ) -> Option<u64> {
        if !self.is_accepting() {
            return None;
        }
        let mut supervisor = self.supervisor.lock();
        if !self.is_accepting() || !control.activate() {
            return None;
        }
        let id = supervisor.next_task;
        supervisor.next_task += 1;
        supervisor.tasks.insert(id, ManagedTask { control, abort });
        Some(id)
    }

    pub(crate) fn task_finished(&self, id: u64) {
        let mut supervisor = self.supervisor.lock();
        supervisor.tasks.remove(&id);
        #[cfg(feature = "tracing")]
        tracing::trace!(task_id = id, "managed resource task finished");
        if !self.is_accepting() && supervisor.tasks.is_empty() {
            self.phase
                .store(DomainPhase::Terminated as u8, Ordering::Release);
            drop(supervisor);
            self.quiescent.notify_waiters();
        }
    }

    pub(crate) fn try_acquire<R: Resource>(&self, entry: &Arc<Entry<R>>) -> bool {
        if !self.is_accepting() || !entry.control.is_active() {
            return false;
        }
        self.is_accepting() && entry.control.is_active()
    }

    pub(crate) fn is_accepting(&self) -> bool {
        self.phase.load(Ordering::Acquire) == DomainPhase::Running as u8
    }

    pub(crate) fn cancel(&self) {
        let supervisor = self.supervisor.lock();
        if self
            .phase
            .compare_exchange(
                DomainPhase::Running as u8,
                DomainPhase::ShuttingDown as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                active_tasks = supervisor.tasks.len(),
                "domain shutdown started"
            );
            self.shutdown.cancel();
        }
        if self.phase.load(Ordering::Acquire) == DomainPhase::Terminated as u8 {
            return;
        }
        let controls = supervisor
            .tasks
            .values()
            .map(|task| task.control.clone())
            .collect::<Vec<_>>();
        if supervisor.tasks.is_empty() {
            self.phase
                .store(DomainPhase::Terminated as u8, Ordering::Release);
            self.quiescent.notify_waiters();
        }
        drop(supervisor);
        for control in controls {
            control.cancel();
        }
    }

    pub(crate) async fn terminate(&self) {
        self.cancel();
        let aborts = self
            .supervisor
            .lock()
            .tasks
            .values()
            .map(|task| task.abort.clone())
            .collect::<Vec<_>>();
        for abort in aborts {
            abort.abort();
        }
        self.shutdown().await;
    }

    pub(crate) async fn shutdown(&self) {
        self.cancel();
        loop {
            let notified = self.quiescent.notified();
            if self.phase.load(Ordering::Acquire) == DomainPhase::Terminated as u8 {
                return;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn active_tasks(&self) -> usize {
        self.supervisor.lock().tasks.len()
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;
    use std::{
        convert::Infallible,
        sync::{Barrier, atomic::AtomicBool},
    };

    use crate::{ResourceSpec, Singleton};

    struct Canonical;

    impl Resource for Canonical {
        type Input = ();
        type Error = Infallible;
        type Placement = Singleton;

        async fn build((): ()) -> ResourceSpec<Self, Self::Error> {
            ResourceSpec::new(Self, |_| async { Ok(()) })
        }
    }

    #[test]
    fn cleanup_cannot_detach_a_slot_while_it_is_being_claimed() {
        let domain = Arc::new(Domain::new());
        let registry = domain.registry::<Canonical>();
        let RegistryClaim::Owner { slot, generation } = registry.claim(&(), &domain, false) else {
            panic!("first claim must own construction");
        };
        assert_eq!(generation, 1);
        *slot.state.lock() = RegistrySlotState::Active {
            generation,
            entry: Weak::new(),
        };

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let claimant = {
            let domain = domain.clone();
            let registry = registry.clone();
            let entered = entered.clone();
            let release = release.clone();
            std::thread::spawn(move || {
                let claim = registry.claim_with_hook(&(), &domain, || {
                    entered.wait();
                    release.wait();
                });
                let RegistryClaim::Owner { slot, generation } = claim else {
                    panic!("the dead active generation should be replaced");
                };
                assert_eq!(generation, 2);
                slot
            })
        };

        entered.wait();
        let cleanup_started = Arc::new(AtomicBool::new(false));
        let cleanup = {
            let registry = registry.clone();
            let slot = slot.clone();
            let cleanup_started = cleanup_started.clone();
            std::thread::spawn(move || {
                cleanup_started.store(true, Ordering::Release);
                registry.remove_if_same(&(), &slot, 1);
            })
        };
        while !cleanup_started.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        release.wait();

        let claimed_slot = claimant.join().unwrap();
        cleanup.join().unwrap();
        let registered_slot = registry.slot(&()).expect("claimed slot remains registered");
        assert!(Arc::ptr_eq(&claimed_slot, &registered_slot));
        assert!(matches!(
            registry.claim(&(), &domain, false),
            RegistryClaim::Wait(_)
        ));
    }

    #[test]
    fn loom_claim_and_cleanup_have_one_registry_linearization_point() {
        loom::model(|| {
            use loom::sync::{
                Arc, Mutex,
                atomic::{AtomicUsize, Ordering},
            };

            #[derive(Clone, Copy)]
            struct State {
                generation: u64,
                starting: bool,
            }

            let old = Arc::new(Mutex::new(State {
                generation: 1,
                starting: false,
            }));
            let registry = Arc::new(Mutex::new(Some(old)));
            let owners = Arc::new(AtomicUsize::new(0));

            let acquire = {
                let registry = registry.clone();
                let owners = owners.clone();
                loom::thread::spawn(move || {
                    let mut map = registry.lock().unwrap();
                    let slot = map
                        .get_or_insert_with(|| {
                            owners.fetch_add(1, Ordering::SeqCst);
                            Arc::new(Mutex::new(State {
                                generation: 1,
                                starting: true,
                            }))
                        })
                        .clone();
                    let mut state = slot.lock().unwrap();
                    if !state.starting {
                        state.generation += 1;
                        state.starting = true;
                        owners.fetch_add(1, Ordering::SeqCst);
                    }
                })
            };

            let cleanup_and_acquire = {
                let registry = registry.clone();
                let owners = owners.clone();
                loom::thread::spawn(move || {
                    let mut map = registry.lock().unwrap();
                    if map
                        .as_ref()
                        .is_some_and(|slot| slot.lock().unwrap().generation == 1)
                    {
                        *map = None;
                    }
                    if map.is_none() {
                        *map = Some(Arc::new(Mutex::new(State {
                            generation: 1,
                            starting: true,
                        })));
                        owners.fetch_add(1, Ordering::SeqCst);
                    }
                })
            };

            acquire.join().unwrap();
            cleanup_and_acquire.join().unwrap();
            assert_eq!(owners.load(Ordering::SeqCst), 1);
        });
    }
}
