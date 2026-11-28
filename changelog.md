# Changelog

## 0.0.13

### Starting actors

Replace actor builders with `AktorNew { name, kind, start, end, hooks, capacity, stuck }`. Use `AktorInit::Ready(async || state)` for an infallible start and `AktorInit::Try(open_database)` for a fallible one. Pass `end` directly. Start, end and loop errors can return an `ErResult` wherever the result converts to `(E, String)`. Use `AktorHooks::NONE` for no hooks and `()` for an unused per call hook. Capacities take `NonZeroUsize`, intervals use `AktorInterval::new(every, callback)?`, and custom runners take `AktorRunner<'_, State>`.

Replace `aktor_start_with_policy` with `aktor_start(AktorSetup { actors, shutdown, grace, stuck })`. Choose `AktorGroupStuck::KeepRunning` or, on native groups, `AbortProcess(())`. Starting returns the group immediately. Replace direct handle fields with `started.handles()` to borrow or `started.into_parts()` to move them. Keep the owner alive while using its handles. Calls queue while start runs.

For a dynamic spawn, use `group.spawn(setup).ready().await?` to take its handles. To queue calls before startup finishes, use `let (startup, handles) = group.spawn(setup).into_parts()?;` and await `startup.receipt()`. Match `AktorStartError::Refused { reason }`, `Initializer { error, startup }` or `Failed { reason, startup }`. Both launched cases carry actor IDs, init evidence and the rollback result.

### Calls and background work

Replace `op::request(&actor, x)` with `op(&actor, x)` and `op::direct(&mut state, x)` with `op(&mut state, x)`. Use `.into_future()` to poll a call manually. Replace `.admit().await?` with `.send().await?`, then replace `reply.receive_settled().await` with `reply.await?`. Replace `call.settle().await` with `call.await?`. For an operation with its own error, `call.await??` handles both delivery and the operation.

Add `#[aktor(latest)]` to operations used with `.latest()`. Store `op::Sender` in place of `op::LatestSender`. The stream drains its final value on orderly close and returns one delivery error on loss.

Pass `AktorJob::DropAtStop` or `FinishBeforeEnd` to `aktor_spawn(work, policy)?`, `actor.spawn(work, policy)?` or `group.spawn_task(work, policy)?`. Actor jobs share a bound on unfinished work. A failed job receipt carries the same ID as its report row. Code calling the free spawn functions outside a compatible actor receives `AktorSpawnError::NoOwner`. Keep the guard from `stopping.on_stop(callback)` while a blocking loop needs waking.

Replace `testing::call_async` with `testing::call(&actor, async |state| ..)` and chained mock handlers with `Preset::new([op::handler(..)])`.

### Stopping and reporting

Replace `stop().await.report` with `stop().await` for the grace snapshot. Replace `stop().await.wait_released()` with `stop_released().await?` before reopening files or devices. Actors stop in reverse order. Under `Leave`, calls and cleanup may continue after grace. A group dropped without requesting a snapshot gives its shutdown callback the final report. A lost publisher returns `AktorPublicationLost` from the completion.

Use `report.actor(id)` for startup and retirement evidence. A loop's `result().await` returns its value or a failure with an ID. Replace `failed_except` and `render(skip)` by comparing that ID with `row.id` when combining reports. `report.into_result()` counts the stop cause and every failure nobody received. Running past grace alone can still give a clean result.

### Workers and remote peers

Replace `Worker<State, Role, Error, Ready>` with `Worker<Contract>` and put `Required` or `Optional` in the setup's `availability` field. Replace worker constructors with `AktorWorkerNew` in a group. Write the build, capacity and nonzero byte budget in its options. Await the worker's own `ready()` for its typed payload. Match `WorkerStartError::Initializer { error, .. }` in place of reading `failure.init`. Serve with `serve_setup::<Contract>` and return `AktorReady { state, ready }` when the page needs a payload. A `before_shutdown` join reports whether the external work stopped.

Use `completion.cleanup()` to inspect end status and `take()` on an error receipt to mark it delivered. `completion.wait_cleanup().await` consumes the result directly. Use `completion.report().await` for the typed lifecycle report and `wait_retired().await` for final retirement.

Replace `#[aktor(data)]` and the Serde codec with `#[aktor]` and `AktorData` on wire types. Use `#[aktor(remote)]` for network exposure. The page compiles signatures and the worker compiles the bodies with `wasm_browser_worker_server`. Select one state's bodies with `--cfg aktor_serve="StateName"`. Wire types can derive or implement `AktorData` in builds without transport. Use `#[aktor(max_len = N)]` for bounded fields.

Replace `connect` and `listen` with `Client::new(address, options)` and `Server::listen(address, actor, options).await`. Unix listeners take `Address::Unix(UnixListenerOptions { path, permissions })`. TCP and UDP take just their socket address. Set `max_connections` and `AktorRetryOptions::attempts` to nonzero values. Read failed casts from `server.shutdown().await` in place of `server.errors()`.

Custom codecs return `(DataError, String)` from `encode` and `decode`. Match `DataError.reason` for built in limits and use `DataError::new(message)` for your own codec errors.

Rebuild both peers together, including browser pages and workers. Failure IDs carry the producer identity on the wire so remote call errors can be matched against the originating server's report. Bare metal and standalone Wasm hosts supply a random boot identity with `FailureId::set_producer`.

## 0.0.12

Fix cancel/cleanup.

## 0.0.11

Fixed lost startup errors and shutdown hangs. Faster native calls and AktorData encoding.

## 0.0.10

Requires Rust 1.95. Updated parking_lot to 0.13.

Optional browser workers choose failure behavior during setup. Their ordinary calls return delivery errors.

Fixed shutdown deadlocks involving queued calls. Lifecycle code can use `admit()` and `receive_settled()` to get a call's outcome before cleanup finishes.

Browser shutdown now returns `Retirement`. Cleanup that finishes with an error still confirms retirement. Interrupted cleanup leaves retirement unconfirmed. Attach external joins during construction.

`OwnerError::physically_joined()` tells you whether the native thread was joined, including after a panic.

Renamed:

- `Actor` to `AktorControl`, accessed through `.control` instead of `.actor`.
- `ActorError` and `ActorResult` to `AktorStateError` and `AktorStateResult`.
- `ActorFailure` to `AktorFailureReport`.
- `ActorOutcome` to `AktorOutcome`.

## 0.0.9

`BrowserThread` shares Wasm memory, so you can pass `Send` values without encoding them.

`aktor-remote` calls `#[aktor(remote)]` functions over Unix sockets, TCP and optional UDP.

Workers and remote calls use `AktorData`. For Serde, enable `serde` and use `#[aktor(serde)]`. Rebuild both sides to use the new encoding.

Test presets let you override function responses.

`ActorArgs` is now `AktorArgs`.

## 0.0.8

Timeouts keep their deadline during shutdown.

Fixed worker registration in release builds.

Saved reports use `AktorDiagnostic` and include startup rollback. Use `.error` for lifecycle failures and `.diagnostics` for worker setup and cleanup reports.

Group control returns `AktorGroupError`. `run()` returns `AktorRunError`. `AktorShutdownReport` no longer implements `Error`.

## 0.0.7

Lifecycle errors print a short message by default. Use `{err:#}` for full diagnostics. Fixed missing and repeated diagnostics from setup and cleanup.

## 0.0.6

Fixed startup and cleanup reporting, worker failures and shutdown after the caller runtime exits.

## 0.0.5

Typed setup and shutdown hooks, removed the public try calls.

Fixed shutdown cleanup.

## 0.0.4

Complete redesign.

## 0.0.3

Redesign.

## 0.0.2

Fixed macro name collisions.

## 0.0.1

Initial version.
