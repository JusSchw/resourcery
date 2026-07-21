use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use tokio_util::sync::CancellationToken;

use crate::{Placement, Resource, lifecycle::Control, reference::Entry};

pub(crate) struct Registry<R: Resource> {
    pub(crate) entries: HashMap<<<R as Resource>::Placement as Placement<R>>::Key, Weak<Entry<R>>>,
}

impl<R: Resource> Default for Registry<R> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

pub(crate) struct Domain {
    pub(crate) registries: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    controls: Mutex<Vec<Weak<Control>>>,
    pub(crate) shutdown: CancellationToken,
}

impl Domain {
    pub(crate) fn new() -> Self {
        Self {
            registries: Mutex::new(HashMap::new()),
            controls: Mutex::new(Vec::new()),
            shutdown: CancellationToken::new(),
        }
    }

    pub(crate) fn track(&self, control: &Arc<Control>) {
        self.controls.lock().unwrap().push(Arc::downgrade(control));
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
        let mut controls = self.controls.lock().unwrap();
        controls.retain(|weak| {
            if let Some(control) = weak.upgrade() {
                control.cancel();
                true
            } else {
                false
            }
        });
    }
}
