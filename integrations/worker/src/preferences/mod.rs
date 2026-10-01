use serde::{Deserialize, Serialize};

pub mod read;
pub mod write;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageError {
    Sql(String),
    Missing(String),
}
impl From<rusqlite::Error> for StorageError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error.to_string())
    }
}
