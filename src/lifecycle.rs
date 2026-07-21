use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Active,
    Cancelling,
    Finished,
}

struct Lifecycle {
    leases: usize,
    phase: Phase,
}

pub(crate) struct Control {
    lifecycle: Mutex<Lifecycle>,
    pub(crate) cancellation: CancellationToken,
}

impl Control {
    pub(crate) fn new() -> Self {
        Self {
            lifecycle: Mutex::new(Lifecycle {
                leases: 1,
                phase: Phase::Active,
            }),
            cancellation: CancellationToken::new(),
        }
    }

    pub(crate) fn acquire(&self) -> bool {
        let mut state = self.lifecycle.lock().unwrap();
        if state.phase != Phase::Active || state.leases == 0 {
            return false;
        }
        state.leases += 1;
        true
    }

    pub(crate) fn clone_lease(&self) {
        let mut state = self.lifecycle.lock().unwrap();
        assert!(state.leases > 0, "an existing resource lease must be live");
        state.leases += 1;
    }

    pub(crate) fn release(&self) {
        let cancel = {
            let mut state = self.lifecycle.lock().unwrap();
            state.leases -= 1;
            if state.leases == 0 && state.phase == Phase::Active {
                state.phase = Phase::Cancelling;
                true
            } else {
                false
            }
        };
        if cancel {
            self.cancellation.cancel();
        }
    }

    pub(crate) fn finish(&self) {
        self.lifecycle.lock().unwrap().phase = Phase::Finished;
    }

    pub(crate) fn cancel(&self) {
        let cancel = {
            let mut state = self.lifecycle.lock().unwrap();
            if state.phase == Phase::Active {
                state.phase = Phase::Cancelling;
                true
            } else {
                false
            }
        };
        if cancel {
            self.cancellation.cancel();
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.lifecycle.lock().unwrap().phase == Phase::Active
    }
}
