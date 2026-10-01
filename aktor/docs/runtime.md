# Runtime details

## Requests / replies

send() queues the call and gives you a reply to await later. try_send() returns Full(request) or Closed(request) so you can try again. cast() skips the reply, only for functions returning ().

Dropping or timing out a reply leaves queued work running. Drop the last handle and queued calls finish before cleanup. downgrade()/upgrade() gives you a weak handle when you don't want to keep it alive.

## Pause / resume / replace

```rust
database.actor.pause().await?;
database.actor.resume(Connection::open_in_memory).await?;
```

pause finishes queued calls and closes the resource, resume opens a new one. replace(setup) does both. Calls while paused are rejected, failed setup or cleanup leaves it paused.

shutdown().await finishes queued calls and cleanup. Once started it keeps going if you stop awaiting it, and completion.wait().await gets the result later. Keep your tokio runtime running until it's done.

## checked()

For handling a closed actor yourself:

```rust
let result = insert_user::request(&database, "Katten".into()).checked().await?;
let id = result?;
```

First ? is for the actor, second is for your function's result. Ordinary calls assume the actor is alive and panic when they can't get a reply. Errors returned by your function come back as usual.

CallError tells you what happened:

NotAdmitted: wasn't queued.

Discarded: queued, never started.

OutcomeUnknown: started, lost its reply.

Superseded: replaced by a newer latest() call.

A lost reply can mean a write already happened, don't blindly retry it.

## Panics

FailurePolicy::Unwind attempts cleanup then unwinds the actor's thread/task. Abort attempts cleanup then ends the process. shutdown lets you supply your own hook. Cleanup gets whatever state the panicking code left behind.

Cancelling a runner can't await cleanup. With panic = "abort", the process ends immediately.

## Function arguments

Native queued arguments need Send + 'static, local calls can borrow. Outputs need Send + 'static too. Return owned values.

For nested calls, pass the resource you already have. Calling through that actor's handle queues work behind yourself and can deadlock. It keeps the state across awaits, so the next call waits until yours finishes.

Leave the generated target inferred with generics, like query::<User, _>(&database, id). Put cfg on the function, attributes on arguments aren't supported.

[Browser setup](../../integrations/worker/README.md)

[Embassy setup](../../integrations/embassy/README.md)
