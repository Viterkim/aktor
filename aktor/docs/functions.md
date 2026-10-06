# aktor

```rust
use aktor::*;
use rusqlite::{Connection, Result};

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])?;

    Ok(db.last_insert_rowid())
}

let id = insert_user(&database, "Katten".into()).await?;
```

Pass a handle to queue the call, or the connection to run it right there. Calls run one after the other and return your function's own output. If an actor dies, its group starts closing.

[Setup](../../README.md#counter)

[Request options](examples.md)

[Shutdown](runtime.md)
