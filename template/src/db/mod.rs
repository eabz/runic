use crate::config::EngineConfig;
use std::{error::Error, fmt};

pub mod sql;
pub mod models;

#[derive(Debug)]
pub enum DbError {
    UnsupportedBackend(String),
    NotImplemented(&'static str),
    Initialization(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::UnsupportedBackend(name) => {
                write!(f, "Unsupported database backend `{name}`")
            }
            DbError::NotImplemented(name) => {
                write!(f, "{name} backend is not implemented yet")
            }
            DbError::Initialization(msg) => f.write_str(msg),
        }
    }
}

impl Error for DbError {}

pub enum DatabaseConnection {
    Postgres(sql::PostgresHandle),
    Sqlite(sql::SqliteHandle),
}

pub trait DatabaseAdapter: Send + Sync {
    fn backend(&self) -> &'static str;
    fn connect(&self) -> Result<DatabaseConnection, DbError>;
}

pub struct Database {
    adapter: Box<dyn DatabaseAdapter>,
}

impl Database {
    pub fn from_config(engine: &EngineConfig) -> Result<Self, DbError> {
        let adapter: Box<dyn DatabaseAdapter> =
            match engine.db.to_lowercase().as_str() {
                "postgres" | "postgresql" => {
                    Box::new(sql::PostgresDatabase::default())
                }
                "sqlite" | "sqlite3" => {
                    Box::new(sql::SqliteDatabase::default())
                }
                other => {
                    return Err(DbError::UnsupportedBackend(
                        other.to_owned(),
                    ));
                }
            };

        Ok(Self { adapter })
    }

    pub fn backend(&self) -> &'static str {
        self.adapter.backend()
    }

    pub fn connect(&self) -> Result<DatabaseConnection, DbError> {
        self.adapter.connect()
    }
}
