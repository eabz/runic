use crate::config::RunicConfig;
use std::{error::Error, fmt};

pub mod models;
pub mod schema;
mod sql;

pub use sql::{PgPool, SqlitePool};

#[derive(Debug)]
pub enum DbError {
    UnsupportedBackend(String),
    Configuration(String),
    Initialization(String),
    Migration(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::UnsupportedBackend(name) => {
                write!(f, "Unsupported database backend `{name}`")
            }
            DbError::Configuration(msg) => f.write_str(msg),
            DbError::Initialization(msg) => f.write_str(msg),
            DbError::Migration(msg) => f.write_str(msg),
        }
    }
}

impl Error for DbError {}

pub enum DatabaseHandle {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl DatabaseHandle {
    pub fn as_postgres(&self) -> Option<&PgPool> {
        match self {
            DatabaseHandle::Postgres(pool) => Some(pool),
            DatabaseHandle::Sqlite(_) => None,
        }
    }

    pub fn as_sqlite(&self) -> Option<&SqlitePool> {
        match self {
            DatabaseHandle::Postgres(_) => None,
            DatabaseHandle::Sqlite(pool) => Some(pool),
        }
    }
}

pub struct Database {
    backend: String,
    handle: DatabaseHandle,
}

impl Database {
    pub fn connect(config: &RunicConfig) -> Result<Self, DbError> {
        match config.engines.db.to_lowercase().as_str() {
            "postgres" | "postgresql" => {
                let pool = sql::connect_postgres(config)?;
                Ok(Self {
                    backend: "postgres".to_owned(),
                    handle: DatabaseHandle::Postgres(pool),
                })
            }
            "sqlite" | "sqlite3" => {
                let pool = sql::connect_sqlite(config)?;
                Ok(Self {
                    backend: "sqlite".to_owned(),
                    handle: DatabaseHandle::Sqlite(pool),
                })
            }
            other => Err(DbError::UnsupportedBackend(other.to_owned())),
        }
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn handle(&self) -> &DatabaseHandle {
        &self.handle
    }
}
