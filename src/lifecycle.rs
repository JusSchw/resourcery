use std::sync::atomic::{AtomicU8, Ordering};

use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Phase {
    Starting,
    Active,
    Cancelling,
    Finished,
}

pub(crate) struct Control {
    phase: AtomicU8,
    pub(crate) cancellation: CancellationToken,
}

impl Control {
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        Self {
            phase: AtomicU8::new(Phase::Starting as u8),
            cancellation,
        }
    }

    pub(crate) fn activate(&self) -> bool {
        self.phase
            .compare_exchange(
                Phase::Starting as u8,
                Phase::Active as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn is_active(&self) -> bool {
        self.phase.load(Ordering::Acquire) == Phase::Active as u8
    }

    pub(crate) fn finish(&self) {
        self.phase.store(Phase::Finished as u8, Ordering::Release);
    }

    pub(crate) fn cancel(&self) {
        let mut current = self.phase.load(Ordering::Acquire);
        loop {
            if current != Phase::Starting as u8 && current != Phase::Active as u8 {
                return;
            }
            match self.phase.compare_exchange_weak(
                current,
                Phase::Cancelling as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.cancellation.cancel();
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }
}
