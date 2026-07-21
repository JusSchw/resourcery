use std::{fmt, sync::Arc};

/// An error encountered while acquiring a resource generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// Create-only spawning found an active canonical generation.
    Occupied,
    /// The containing resource domain has begun shutdown.
    ShuttingDown,
    /// Resource construction panicked before the generation was published.
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

/// Failure to acquire or execute the root resource.
#[derive(Debug)]
pub enum RunError<E> {
    /// The runtime could not acquire the root resource.
    Acquire(AcquireError),
    /// The root task returned its declared resource error.
    Resource(Arc<E>),
    /// The root resource task panicked.
    Panicked(Arc<str>),
    /// The root resource task was forcibly terminated by its executor.
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
