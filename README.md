# Resourcery

Resourcery is a Tokio-based framework for building applications from typed,
long-lived tasks. A resource consists of a public interface and an asynchronous
task. Code accesses that interface through a `ResourceRef`; as long as at least
one strong reference exists, the resource is needed. Dropping the final strong
reference requests cancellation of its task.

Resources can retain references to other resources. Those normal Rust values
form the application's dependency graph, so shared dependencies and teardown
order follow naturally from ownership rather than a separate lifecycle graph.

## Core concepts

- `Resource` declares the input, error type, placement, interface, and task.
- `ResourceSpec` joins a public interface to the task implementing it.
- `ResourceRef` is an owned, typed lease that keeps one generation live.
- `ResourceCompletion` observes one terminal outcome without keeping it live.
- `ResourceContext` lets a task acquire resources and observe cancellation.
- `ResourceRuntime` owns an isolated domain and runs its root resource.
- `Unique`, `Singleton`, and `Keyed` define how requests map to generations.

The registry stores weak references in per-identity construction slots. It can
discover and deduplicate canonical resources, but it never keeps them alive.
`ResourceRef` and `ResourceContext` are ordinary movable, shareable runtime
handles; they deliberately have no phantom lexical lifetime.

## Defining and running resources

The following application acquires one counter identified by name. The counter
exposes an atomic value as its interface and runs until its final reference is
dropped.

```rust
use std::{convert::Infallible, sync::atomic::{AtomicU64, Ordering}};
use resourcery::{
    AcquireError, Keyed, Keyer, Resource, ResourceRuntime, ResourceSpec, Unique,
};

struct Counter {
    value: AtomicU64,
}

impl Counter {
    fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    fn load(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

struct CounterInput {
    name: String,
    initial: u64,
}

struct CounterByName;

impl Keyer<Counter> for CounterByName {
    type Key = String;

    fn key(input: &CounterInput) -> Self::Key {
        input.name.clone()
    }
}

impl Resource for Counter {
    type Input = CounterInput;
    type Error = Infallible;
    type Placement = Keyed<CounterByName>;

    fn build(input: Self::Input) -> ResourceSpec<Self, Self::Error> {
        let interface = Counter {
            value: AtomicU64::new(input.initial),
        };

        ResourceSpec::new(interface, |cx| async move {
            cx.cancelled().await;
            Ok(())
        })
    }
}

struct Application;

impl Resource for Application {
    type Input = ();
    type Error = AcquireError;
    type Placement = Unique;

    fn build((): Self::Input) -> ResourceSpec<Self, Self::Error> {
        ResourceSpec::new(Application, |cx| async move {
            let counter = cx.get_or_spawn::<Counter>(CounterInput {
                name: "requests".into(),
                initial: 0,
            })?;

            counter.increment();
            assert_eq!(counter.load(), 1);
            Ok(())
        })
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
ResourceRuntime::new().run::<Application>(()).await?;
# Ok(())
# }
```

Larger applications commonly define their own root error type and implement
`From<AcquireError>` for it.

## Placement and identity

`Unique` creates a new generation on every `spawn` call. It is suitable for
jobs, sessions, and operations that must not be reused. Unique resources do not
participate in `get` or `all` discovery.

`Singleton` provides at most one active generation of a resource type in a
domain. Its key is `()`.

`Keyed<K>` provides at most one active generation for each key. Implement
`Keyer<R>` on a marker type to derive that key from `R::Input`. Only immutable
identity belongs in the key. Other input fields are creation parameters: when
`get_or_spawn` finds an existing generation, it ignores those fields rather
than reconfiguring the resource.

Canonical identity is generational. Once a singleton or keyed generation is
cancelling or finished, discovery no longer returns it and a later acquisition
may establish a replacement under the same identity. Existing references stay
attached to the generation they originally acquired.

## Acquisition operations

| Operation | Unique placement | Canonical placement |
| --- | --- | --- |
| `spawn(input)` | Always creates | Creates or returns `Occupied` |
| `get(key)` | Always absent | Retrieves an active generation only |
| `get_or_spawn(input)` | Creates | Atomically retrieves or creates |
| `all()` | Empty | Returns all active generations of the type |

`status(key)` provides a non-blocking distinction between `Absent`, `Starting`,
and `Active`. `get_or_spawn` waits behind the per-identity starting slot, so all
concurrent callers receive the one generation that wins construction.

Every returned strong reference counts as a lease. In particular, retaining the
vector returned by `all` keeps every listed resource live.

## Cancellation and completion

Cancellation begins when the last `ResourceRef` is dropped or when
`ResourceRuntime::shutdown` is called. Resource tasks should regularly select
on or await `ResourceContext::cancelled` and then perform bounded cleanup.
Cancellation is cooperative: the framework does not forcibly abort a task.

Create a non-owning observer with `ResourceRef::completion`. It is clonable,
does not count as a lease, and remains usable after the interface is gone:

```rust
let completion = resource.completion();
drop(resource);
let outcome = completion.wait().await;
```

Every observer receives the same `ResourceOutcome`: `Completed`, `Failed`,
`Panicked`, or `Aborted`. Declared errors and panic messages are shared in an
`Arc`, so the resource error need not implement `Clone`. `ResourceRef::finished`
is a convenience that waits while retaining that reference and is unsuitable
for a task that exits only after its final lease is released.

## Dependencies and cycles

A resource task can acquire another resource and retain its `ResourceRef` in
the task future or its public interface. This creates a strong liveness edge.
Shared dependencies remain alive until every dependent releases them.

Avoid cycles of strong references: each member would keep the next alive. Use
`WeakResourceRef`, message channels, or a separate coordinator for back-links.
A weak reference can be upgraded only while its generation is active.

## Blocking computation

Use `ResourceContext::compute` for one-shot synchronous or CPU-heavy work. It
delegates to Tokio's blocking pool and returns the resulting value. It is not a
resource: it has no identity, interface, registry entry, or independent
lifecycle. Async I/O should remain in the managed async task rather than inside
`compute`.

## Runtime domains

Each `ResourceRuntime` owns an independent registry, identity space, and
cancellation domain. Canonical resources are shared only inside one runtime.
Create separate runtimes when tests, tenants, or subsystems require isolation.

`run` creates the root with its declared placement and maps its terminal outcome
to `RunError`. `cancel` atomically closes acquisition and requests cooperative
cancellation without waiting. `shutdown().await` additionally waits for all
managed tasks to finish. `terminate().await` aborts tasks that cannot cooperate
and reports `Aborted` to their completion observers. These operations are
idempotent; once shutdown starts, the runtime cannot be restarted.
