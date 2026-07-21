use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Starting,
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
                phase: Phase::Starting,
            }),
            cancellation: CancellationToken::new(),
        }
    }

    pub(crate) fn activate(&self) -> bool {
        let mut state = self.lifecycle.lock().unwrap();
        if state.phase != Phase::Starting {
            return false;
        }
        state.phase = Phase::Active;
        true
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
        assert!(
            state.leases > 0 && state.phase == Phase::Active,
            "new leases may only be created for an active resource"
        );
        state.leases += 1;
    }

    pub(crate) fn release(&self) {
        let cancel = {
            let mut state = self.lifecycle.lock().unwrap();
            state.leases -= 1;
            if state.leases == 0 && matches!(state.phase, Phase::Starting | Phase::Active) {
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
            if matches!(state.phase, Phase::Starting | Phase::Active) {
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
}
