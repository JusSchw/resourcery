use std::{fmt, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    Occupied,
    ShuttingDown,
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

#[derive(Debug)]
pub enum RunError<E> {
    Acquire(AcquireError),
    Resource(Arc<E>),
    Panicked(Arc<str>),
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
