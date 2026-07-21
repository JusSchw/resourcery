#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod context;
mod domain;
mod error;
mod lifecycle;
mod placement;
mod reference;
mod resource;
mod runtime;

pub use context::ResourceContext;
pub use error::{AcquireError, RunError};
pub use placement::{Keyed, Keyer, Placement, Singleton, Unique};
pub use reference::{ResourceRef, WeakResourceRef};
pub use resource::{Never, Resource, ResourceSpec};
pub use runtime::ResourceRuntime;
