use std::{fmt, sync::Arc};

/// Failure to acquire or establish a resource generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AcquireError {
    /// A non-waiting operation found a canonical identity starting or active.
    #[error("the canonical resource is already live or starting")]
    Occupied,
    /// The domain has begun shutdown and no longer accepts acquisitions.
    #[error("the resource domain is shutting down")]
    ShuttingDown,
    /// [`Resource::build`](crate::Resource::build) panicked.
    #[error("resource construction panicked")]
    ConstructionPanicked,
}

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

impl<E: std::fmt::Display> std::fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acquire(error) => error.fmt(f),
            Self::Resource(error) => error.fmt(f),
            Self::Panicked(message) => write!(f, "root resource panicked: {message}"),
            Self::Aborted => f.write_str("root resource was aborted"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for RunError<E> {}
