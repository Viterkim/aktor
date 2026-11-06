# Changelog

## 0.0.12

Fix cancel/cleanup.

## 0.0.11

Fixed lost startup errors and shutdown hangs. Faster native calls and AktorData encoding.

## 0.0.10

Requires Rust 1.95. Updated parking_lot to 0.13.

Optional browser workers choose failure behavior during setup. Their ordinary calls return delivery errors.

Fixed shutdown deadlocks involving queued calls. Lifecycle code can use `admit()` and `receive_settled()` to get a call's outcome before cleanup finishes.

Browser shutdown now returns `Retirement`. Cleanup that finished with an error is confirmed, interrupted cleanup stays unconfirmed. Attach external joins during construction.

`OwnerError::physically_joined()` tells you whether the native thread was joined, including after a panic.

Renamed:

- `Actor` to `AktorControl`, accessed through `.control` instead of `.actor`.
- `ActorError` and `ActorResult` to `AktorStateError` and `AktorStateResult`.
- `ActorFailure` to `AktorFailureReport`.
- `ActorOutcome` to `AktorOutcome`.

## 0.0.9

BrowserThread shares Wasm memory, pass Send values without encoding them.

aktor-remote calls #[aktor(remote)] functions over Unix sockets, TCP and optional UDP.

Workers and remote calls use AktorData. For Serde, enable serde and use #[aktor(serde)]. Rebuild both sides, the encoding changed.

Test presets let you override function responses.

ActorArgs is now AktorArgs.

## 0.0.8

Timeouts keep their deadline during shutdown.

Fixed worker registration in release builds.

Saved reports use `AktorDiagnostic` and include startup rollback. Use `.error` for lifecycle failures and `.diagnostics` for worker setup and cleanup reports.

Group control returns `AktorGroupError`, `run()` returns `AktorRunError`. `ShutdownReport` no longer implements `Error`.

## 0.0.7

Lifecycle errors print short, use {err:#} for full diagnostics. Fixed missing and repeated setup/cleanup diagnostics.

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
