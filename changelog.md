# Changelog

## 0.0.13

New group setup with typed startup errors. `end` accepts your close function directly and includes its full error message in the shutdown report. Startup, shutdown, remote and optional worker failures come with their report text. Uses Er 0.9. Rebuild the browser page and its workers together for the new startup protocol.

A stuck native group now leaves the app running by default. Choose `AktorShutdownPolicy::AbortProcess` at startup for a hard exit.

Actors stop in reverse startup order. A stuck actor can finish later, and outside work can stop with the group. `started.shutdown()` gives you the group's completion handle. You can await it for the report or keep it to check retirement later.

A browser worker's `wait_retired()` now distinguishes a worker that was killed from one that might still be running.

Fixed stuck calls and startup on Embassy cross-core, stuck local startup and browser worker shutdown. `AktorData` handles recursive types with marker fields.

`Vec<i8>` and `Box<[i8]>` encode and decode faster with the same Postcard bytes. If you send those types through `AktorData` with CBOR yourself, update the reader first. It reads the old signed arrays and the byte strings the new writer produces.

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
