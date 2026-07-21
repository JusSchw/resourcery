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

pub use context::{ResourceContext, ResourceStatus};
pub use error::{AcquireError, RunError};
pub use placement::{CanonicalPlacement, Keyed, Keyer, Placement, Singleton, Unique};
pub use reference::{ResourceCompletion, ResourceOutcome, ResourceRef, WeakResourceRef};
pub use resource::{Never, Resource, ResourceSpec};
pub use runtime::ResourceRuntime;
