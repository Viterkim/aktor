# aktor-macros

Use it through [aktor](../README.md), which reexports `#[aktor]`:

```rust
use aktor::*;
use rusqlite::{Connection, Result};

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])?;

    Ok(db.last_insert_rowid())
}
```

Pass your handle to queue it, or the connection to run it right there. The [examples](../aktor/docs/examples.md) show what else you can do with a request.
