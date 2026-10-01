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

## Calling another query (nested)

Pass the connection you already have. You can put those calls in a transaction too:

```rust
#[aktor]
pub async fn insert_users(db: &mut Connection, names: Vec<String>) -> Result<()> {
    let transaction = db.transaction()?;

    for name in names {
        insert_user(&*transaction, name).await?;
    }

    transaction.commit()
}
```

Calling insert_users with a handle queues the whole thing once.

[Opening / spawning](../../README.md#opening--starting--spawning)

[Extra examples](examples.md)
