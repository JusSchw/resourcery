use std::{fmt, sync::Arc};

/// Failure to acquire or establish a resource generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// `spawn` was used for a canonical identity that is already active.
    Occupied,
    /// The domain has begun shutdown and no longer accepts acquisitions.
    ShuttingDown,
    /// [`Resource::build`](crate::Resource::build) panicked.
    ConstructionPanicked,
}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Occupied => f.write_str("the canonical resource is already live"),
            Self::ShuttingDown => f.write_str("the resource domain is shutting down"),
            Self::ConstructionPanicked => f.write_str("resource construction panicked"),
        }
    }
}

impl std::error::Error for AcquireError {}

/// Failure returned by [`ResourceRuntime::run`](crate::ResourceRuntime::run).
#[derive(Debug)]
pub enum RunError<E> {
    /// The root generation could not be acquired.
    Acquire(AcquireError),
    /// The root task returned its declared error.
    Resource(Arc<E>),
    /// The root task panicked with this message.
    Panicked(Arc<str>),
    /// The root task was forcibly aborted.
    Aborted,
}

impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acquire(error) => error.fmt(f),
            Self::Resource(error) => error.fmt(f),
            Self::Panicked(message) => write!(f, "root resource panicked: {message}"),
            Self::Aborted => f.write_str("root resource was aborted"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for RunError<E> {}
