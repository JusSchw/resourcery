# Resourcery

Resourcery is a Rust framework for building applications from typed, long-lived
resources. A resource combines a public interface, creation input, placement
policy, managed task, and task error type. Code uses the resource through a
strong `ResourceRef`; the same value that grants access also expresses that the
resource is still required.

> Holding means requiring. Dropping means releasing. Placement defines identity.

## The model

Each call that creates a resource establishes one **generation**. A generation
has one interface and one task:

```text
Resource<Input, Error, Placement>
              │ build
              ▼
        generation ───── ResourceRef<R> (strong lease and interface access)
              │
              ├──────── WeakResourceRef<R> (non-owning access)
              └──────── ResourceCompletion<E> (non-owning outcome observer)
```

Cloning a `ResourceRef` adds a lease to the same generation; it never creates a
new resource. Dropping the final lease requests cancellation exactly once.
Cancellation is cooperative, so the generation becomes terminal only after its
task returns, panics, or is aborted. Already-held strong references remain valid
while cancellation is in progress and after termination.

Resources create dependencies simply by retaining strong references to other
resources. No separate lifecycle graph is declared:

```text
Application ──▶ Database ──▶ ConnectionPool
      │
      └───────▶ Metrics ◀──── Worker
```

When `Application` terminates, its owned references are dropped. A dependency
whose final lease disappears is cancelled in turn; shared dependencies remain
live while any lease remains. Strong cycles cannot be reclaimed by reference
counting, so model a back-edge with `WeakResourceRef`, messaging, or a separate
coordinator.

## Defining and running a resource

Implement `Resource` for the public interface, then return the interface and its
task from `ResourceSpec::new`:

```rust
use resourcery::{
    AcquireError, Never, Resource, ResourceOutcome, ResourceRuntime, ResourceSpec,
    Singleton, Unique,
};

struct Configuration {
    environment: String,
}

impl Resource for Configuration {
    type Input = String;
    type Error = Never;
    type Placement = Singleton;

    async fn build(environment: String) -> ResourceSpec<Self, Self::Error> {
        ResourceSpec::new(Self { environment }, |cx| async move {
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

    async fn build((): ()) -> ResourceSpec<Self, Self::Error> {
        ResourceSpec::new(Self, |cx| async move {
            let config = cx.get_or_spawn::<Configuration>("production".into()).await?;
            assert_eq!(config.environment, "production");

            // Observe shutdown without extending Configuration's lifetime.
            let completion = config.completion();
            drop(config); // final lease: request Configuration cancellation
            assert!(matches!(completion.wait().await, ResourceOutcome::Completed));
            Ok(())
        })
    }
}

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let runtime = ResourceRuntime::new();
runtime.run::<Application>(()).await?;
runtime.shutdown().await;
# Ok(())
# }
```

`Resource::build` is asynchronous, so construction can await initialization
without blocking a runtime worker. The interface is published only after build
returns, and the managed task starts only after the generation is registered.
A panic while creating or polling `build` is an acquisition error; a panic while
polling the managed task is a terminal resource outcome.

## Placement and identity

Placement controls how creation requests map to active generations inside one
`ResourceRuntime` domain.

| Placement | Identity | Typical use |
|---|---|---|
| `Unique` | No reusable identity; every `spawn` is independent | jobs, sessions, operations |
| `Singleton` | One active generation per resource type | configuration, telemetry |
| `Keyed<K>` | One active generation per derived key | tenant caches, named databases, worker slots |

For keyed placement, implement `Keyer` and include every immutable
identity-bearing value in its key:

```rust
use resourcery::{Keyed, Keyer, Never, Resource, ResourceSpec};

struct Database { name: String }
struct DatabaseInput { name: String, pool_size: usize }
struct ByName;

impl Keyer<Database> for ByName {
    type Key = String;
    fn key(input: &DatabaseInput) -> Self::Key { input.name.clone() }
}

impl Resource for Database {
    type Input = DatabaseInput;
    type Error = Never;
    type Placement = Keyed<ByName>;

    async fn build(input: Self::Input) -> ResourceSpec<Self, Self::Error> {
        let interface = Self { name: input.name };
        ResourceSpec::new(interface, |cx| async move {
            // `pool_size` would configure this newly created generation.
            let _ = input.pool_size;
            cx.cancelled().await;
            Ok(())
        })
    }
}
```

Inputs with the same key address the same active generation. Values excluded
from the key are **creation input**: they affect only a newly built generation.
Acquiring an existing generation does not reconfigure it; expose runtime changes
through the resource's public interface instead.

Identity is scoped by both resource type and domain. Two runtimes never share
generations, even when type and key are identical.

## Acquisition API

The method communicates whether reuse is acceptable:

| Operation | Meaning | Creates? | Reuses? |
|---|---|---:|---:|
| `cx.spawn::<R>(input).await` | Establish a new generation | yes | no |
| `cx.get::<R>(&key)` | Retrieve an active canonical generation | no | yes |
| `cx.get_or_spawn::<R>(input).await` | Retrieve it or asynchronously wait to establish it | if absent | yes |
| `cx.try_get_or_spawn::<R>(input).await` | Retrieve or establish without waiting for another constructor | if absent | yes |
| `cx.status::<R>(&key)` | Observe absent, starting, or active state | no | active lease |
| `cx.all::<R>()` | Snapshot all active generations of a type | no | active leases |

`spawn` always creates for `Unique`. For `Singleton` and `Keyed`, it is
create-only and returns `AcquireError::Occupied` when the identity is occupied.
Concurrent `get_or_spawn` calls for one identity converge on a single generation.
Waiting callers suspend rather than blocking a Tokio worker. `try_get_or_spawn`
returns `AcquireError::Occupied` if another caller is constructing the identity.

`get`, `status`, `get_or_spawn`, and `try_get_or_spawn` are available only for canonical placements.
`all` also includes unique generations. Discovery storage is weak: it does not
keep a resource alive, but every reference returned to the caller is a new strong
lease.

## Generations and outcomes

Canonical identity is not a permanent instance. After a generation terminates,
a later acquisition may establish another generation under the same key.
Existing references and completion observers stay attached to the old
generation; replacement never retargets them.

Every managed task publishes exactly one `ResourceOutcome`:

- `Completed` — the task returned `Ok(())`.
- `Failed(Arc<E>)` — the task returned its declared error.
- `Panicked(Arc<str>)` — the task panicked.
- `Aborted` — the task was forcibly terminated.

Choose how observation should affect lifetime:

```rust,ignore
// Borrows a strong lease for the duration of the wait.
let outcome = resource.finished().await;

// Does not retain a lease; dropping `resource` may initiate cancellation.
let completion = resource.completion();
drop(resource);
let outcome = completion.wait().await;
```

Completion observers are cloneable multicast observers. All clones see the same
immutable outcome.

## Cancellation and domain shutdown

`ResourceContext::cancelled()` resolves when either the generation is cancelled
or its entire domain begins shutdown. Tasks should then stop accepting work,
resolve pending operations, publish final state as needed, release external
resources, and return.

The runtime offers three shutdown levels:

- `cancel()` signals cooperative cancellation and returns immediately.
- `shutdown().await` signals cancellation and waits for every task; it can wait
  indefinitely for a task that ignores cancellation.
- `terminate().await` signals cancellation, aborts remaining tasks, and waits for
  quiescence. Aborted tasks report `ResourceOutcome::Aborted`.

Once shutdown starts, the domain rejects new acquisition and weak-reference
upgrades. Existing strong references can still be cloned because that does not
perform a new registry acquisition. These operations are idempotent.

## API guide

- `Resource` binds interface, `Input`, `Error`, `Placement`, and construction.
- `ResourceSpec` pairs an interface with its managed task.
- `ResourceRef<R>` is strong interface access and a liveness lease.
- `WeakResourceRef<R>` is non-owning access that may be upgraded while active.
- `ResourceCompletion<E>` observes termination without retaining liveness.
- `ResourceOutcome<E>` describes the immutable terminal result.
- `ResourceContext` provides resource acquisition, discovery, and cancellation.
- `ResourceRuntime` owns the domain and coordinates root execution and shutdown.
- `Unique`, `Singleton`, and `Keyed<K>` define placement and identity.
- `AcquireError` reports creation/acquisition failures; `RunError<E>` reports a
  root resource's acquisition or terminal failure.

## Semantic guarantees

1. One generation exposes one typed interface backed by one managed task.
2. Every `ResourceRef` is one strong lease; cloning preserves generation identity.
3. Registries, supervision, weak references, and completion observers are not leases.
4. Releasing the final lease requests cancellation exactly once.
5. Weak discovery returns only active generations in an accepting domain.
6. Every managed generation produces one stable terminal outcome.
7. Canonical acquisition establishes at most one active generation per identity.
8. Existing references never move to a replacement generation.
9. Retained strong references are the resource dependency graph.
10. Task termination releases owned dependencies and propagates reclamation.
11. Strong dependency cycles require an explicit weak or indirect edge.
12. Acquisition never implicitly reconfigures an existing generation.

The practical consequence is simple: an application's lifecycle can be read from
the resource references its tasks and interfaces retain.
