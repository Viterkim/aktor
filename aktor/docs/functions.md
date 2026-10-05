# aktor

Put `#[aktor]` above your function:

```rust
use aktor::*;
use rusqlite::{Connection, Result};

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])?;

    Ok(db.last_insert_rowid())
}
```

Then pass your handle as the first argument:

```rust
let id = insert_user(&database, "Katten".into()).await?;
```

Calls run one after the other, and your function's errors come back as usual. Functions can live wherever you want, go to definition takes you to the one you wrote.

Keep the actors with your application in an AktorGroup. If one dies, the group starts closing, your queries don't need another Result for that.

Inside another query, pass the connection you already have and it runs right there. Passing the handle again queues it behind yourself. The [examples](examples.md#calling-another-query) show a transaction doing that.

[Opening / spawning](../../README.md#counter)

[Extra examples](examples.md)
