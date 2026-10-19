# Changelog

## 0.0.8

Timeouts keep their deadline during shutdown.

Worker registrations survive optimized builds across dependency archives.

Saved reports use AktorDiagnostic, with startup rollback included (Display just a small msg, Lifecycle and completion outcomes moved to .error, and worker setup/cleanup text moved to .diagnostics).

Group control methods return AktorGroupError with the rejection reason in ordinary Display. Group run() returns AktorRunError. ShutdownReport no longer implements Error.

## 0.0.7

Lifecycle errors uses short Display and Debug but with full diags on `{err:#}` (and fixed missing and repeated setup/cleanup diagnostics).

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
