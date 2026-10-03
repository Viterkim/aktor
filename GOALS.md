# Backend work

## Automatic browser operations

- [x] Register portable #[aktor] functions where they are written, remove worker_routes.
- [x] Keep local and native functions working without Serde.
- [x] Match both state and actor marker when serving operations.
- [x] Prove actor markers through grouped worker startup and shutdown too.
- [x] Compare operation names and argument/result types at startup.
- [x] Prove split modules, actor markers and missing operations in Chromium.
- [x] Run the native and integration checks on Rust 1.89 and the current toolchain.
- [x] Finish browser checks after the signature mismatch fixture.

## Local actors and WASM hosts

- [x] Run the Embassy integration and embedded compilation checks.
- [x] Run local actors in an isolated WASM module without browser APIs or workers.
- [x] Exercise normal calls, typed errors, queue pressure, retained replies and cleanup there.
- [x] Check whether the same module runs in two different hosts.
- [x] Decide whether a separate backend is needed from those results.
- [x] Fix small conveniences the proof actually needs, keep ordinary function outputs.
- [x] Record the host requirements and checks without growing the beginner docs.

Snak stays untouched while its other agent works.

The local backend passed in both Node and Wasmtime with no host imports. Keep one implementation for Embassy and these hosts. The embassy name currently comes from embassy-sync, it doesn't require the Embassy executor. Outputs still require Send + 'static. Local groups use the host's embassy-time driver, the plain WASM proof advances a test clock.

## Remaining 0.0.3 work

## Lifecycle error records

- [x] Let setup and cleanup callbacks return Aktor records with diagnostics and optional typed data.
- [x] Keep typed data on individual actor outcomes, collect plain records at the group boundary.
- [x] Use the same records for native, local and browser lifecycle failures.
- [x] Remove snapshot fields and Er-specific lifecycle formatting from the public API.
- [x] Prove normal error text and full Er reports through actual setup/cleanup collectors.

## Final review

- [x] Review lifecycle, admission, cancellation and shutdown behavior, fix findings and check them.
- [x] Review public calls, backend consistency, transport and docs, fix findings and rerun affected checks.

The remaining release checklist follows. Snak's real application qualification belongs to its agent and is not performed in this workspace.

- [x] Remove public checked calls and types once the ordinary failure path is ready on each backend.
- [x] Return lazy call objects from generated functions, keep direct calls on the resource and caller locations.
- [x] Replace latest(key) with independent latest sessions and bounded pending work.
- [x] Add explicit timeout(duration) waits, remove implicit worker deadlines.
- [x] Make native shutdown start immediately and return an awaitable completion too.
- [x] Provide the application/group lifetime on the local backend, including its host's timer and exit policy.
- [x] Finish the binary Serde transport qualification and soft byte admission budget.
- [ ] Have Snak's agent qualify the real SQLite/Iced close flow after the call API migration.

Both review passes are finished. Core and integration checks pass on Rust 1.89 and the current toolchain, including Clippy and rustdoc. Embassy runs on its host executor and compiles for both embedded targets. The same isolated WASM proof passes in Node and Wasmtime with no imports.

Chromium passed grouped worker failure and cleanup, typed setup/cleanup data, retained replies after worker loss, bounded latest sessions and an 8 MiB SQLite roundtrip. The codec fixtures also preserve a public Er error with report text and snapshot. Snak's real SQLite/Iced close flow remains with its agent.
