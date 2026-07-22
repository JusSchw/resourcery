use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, Ordering},
    },
};

use dashmap::DashMap;
use event_listener::Event;
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

    pub(crate) fn slot_or_insert(
        &self,
        key: ResourceKey<R>,
        slot: impl FnOnce() -> Arc<RegistrySlot<R>>,
    ) -> (Arc<RegistrySlot<R>>, bool) {
        use dashmap::mapref::entry::Entry;
        match self.entries.entry(key) {
            Entry::Occupied(entry) => (Arc::clone(entry.get()), false),
            Entry::Vacant(entry) => {
                let slot = slot();
                entry.insert(slot.clone());
                (slot, true)
            }
        }
    }

    pub(crate) fn remove_if_same(
        &self,
        key: &ResourceKey<R>,
        slot: &Arc<RegistrySlot<R>>,
        generation: u64,
    ) {
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
            RegistrySlotState::Vacant => true,
            RegistrySlotState::Starting { generation }
            | RegistrySlotState::Active { generation, .. } => *generation == expected,
        }
    }
}

pub(crate) enum RegistrySlotState<R: Resource> {
    Vacant,
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
