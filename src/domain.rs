use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{Placement, Resource, lifecycle::Control, reference::Entry};

type ResourceKey<R> = <<R as Resource>::Placement as Placement<R>>::Key;
type RegistryEntries<R> = HashMap<ResourceKey<R>, Arc<RegistrySlot<R>>>;

pub(crate) struct Registry<R: Resource> {
    pub(crate) entries: Mutex<RegistryEntries<R>>,
    pub(crate) live: Mutex<HashMap<u64, std::sync::Weak<Entry<R>>>>,
}

pub(crate) struct RegistrySlot<R: Resource> {
    pub(crate) state: Mutex<RegistrySlotState<R>>,
    pub(crate) changed: std::sync::Condvar,
}

pub(crate) enum RegistrySlotState<R: Resource> {
    Vacant,
    Starting {
        generation: u64,
    },
    Active {
        generation: u64,
        entry: std::sync::Weak<Entry<R>>,
    },
}

impl<R: Resource> Default for Registry<R> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DomainPhase {
    Running,
    ShuttingDown,
    Terminated,
}

struct Supervisor {
    phase: DomainPhase,
    next_task: u64,
    tasks: HashMap<u64, ManagedTask>,
}

struct ManagedTask {
    control: Arc<Control>,
    abort: tokio::task::AbortHandle,
}

pub(crate) struct Domain {
    registries: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
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
            registries: Mutex::new(HashMap::new()),
            supervisor: Mutex::new(Supervisor {
                phase: DomainPhase::Running,
                next_task: 0,
                tasks: HashMap::new(),
            }),
            quiescent: Notify::new(),
            shutdown: CancellationToken::new(),
        }
    }

    pub(crate) fn registry<R: Resource>(&self) -> Arc<Registry<R>> {
        let mut registries = self.registries.lock().unwrap();
        registries
            .entry(TypeId::of::<R>())
            .or_insert_with(|| Box::new(Arc::new(Registry::<R>::default())))
            .downcast_ref::<Arc<Registry<R>>>()
            .expect("a resource type has exactly one registry type")
            .clone()
    }

    /// Atomically admits and activates a task on the running side of shutdown.
    pub(crate) fn register(
        &self,
        control: Arc<Control>,
        abort: tokio::task::AbortHandle,
    ) -> Option<u64> {
        let mut supervisor = self.supervisor.lock().unwrap();
        if supervisor.phase != DomainPhase::Running || !control.activate() {
            return None;
        }
        let id = supervisor.next_task;
        supervisor.next_task += 1;
        supervisor.tasks.insert(id, ManagedTask { control, abort });
        Some(id)
    }

    pub(crate) fn task_finished(&self, id: u64) {
        let terminated = {
            let mut supervisor = self.supervisor.lock().unwrap();
            supervisor.tasks.remove(&id);
            if supervisor.phase == DomainPhase::ShuttingDown && supervisor.tasks.is_empty() {
                supervisor.phase = DomainPhase::Terminated;
                true
            } else {
                false
            }
        };
        if terminated {
            self.quiescent.notify_waiters();
        }
    }

    /// Acquires a lease on the running side of the shutdown boundary.
    pub(crate) fn try_acquire<R: Resource>(&self, entry: &Arc<Entry<R>>) -> bool {
        let supervisor = self.supervisor.lock().unwrap();
        supervisor.phase == DomainPhase::Running && entry.control.acquire()
    }

    pub(crate) fn is_accepting(&self) -> bool {
        self.supervisor.lock().unwrap().phase == DomainPhase::Running
    }

    pub(crate) fn cancel(&self) {
        let controls = {
            let mut supervisor = self.supervisor.lock().unwrap();
            if supervisor.phase == DomainPhase::Terminated {
                return;
            }
            supervisor.phase = DomainPhase::ShuttingDown;
            self.shutdown.cancel();
            if supervisor.tasks.is_empty() {
                supervisor.phase = DomainPhase::Terminated;
                self.quiescent.notify_waiters();
            }
            supervisor
                .tasks
                .values()
                .map(|task| task.control.clone())
                .collect::<Vec<_>>()
        };
        for control in controls {
            control.cancel();
        }
    }

    pub(crate) async fn terminate(&self) {
        self.cancel();
        let aborts = self
            .supervisor
            .lock()
            .unwrap()
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
            if self.supervisor.lock().unwrap().phase == DomainPhase::Terminated {
                return;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn active_tasks(&self) -> usize {
        self.supervisor.lock().unwrap().tasks.len()
    }
}
