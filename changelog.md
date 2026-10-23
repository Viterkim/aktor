# Changelog

## 0.0.9

BrowserThread shares Wasm memory, pass Send values without encoding them.

aktor-remote calls #[aktor(remote)] functions over Unix sockets, TCP and optional UDP.

Workers and remote calls use AktorData. For Serde, enable serde and use #[aktor(serde)]. Rebuild both sides, the encoding changed.

Test presets let you override function responses.

ActorArgs is now AktorArgs.

## 0.0.8

Timeouts keep their deadline during shutdown.

Worker registrations survive optimized builds across dependency archives.

Saved reports use AktorDiagnostic and include startup rollback. Display is short, use .error for lifecycle outcomes and .diagnostics for worker setup/cleanup reports.

Group control returns AktorGroupError, run() returns AktorRunError. ShutdownReport no longer implements Error.

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

Cut down the docs and cleaned up the code.

## 0.0.2

Fixed macro name collisions.

## 0.0.1

Initial version.
