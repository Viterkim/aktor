# aktor

```rust
#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> rusqlite::Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])?;

    Ok(db.last_insert_rowid())
}

let id = insert_user(&database, "Katten".into()).await?;
```

Pass a handle to queue the call, or the connection to run it right there. Calls run one after the other and return your function's own output.

[Readme](../../README.md)
