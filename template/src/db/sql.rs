use super::{DatabaseAdapter, DatabaseConnection, DbError};

#[derive(Default)]
pub struct PostgresDatabase;

#[derive(Debug, Clone)]
pub struct PostgresHandle;

#[derive(Default)]
pub struct SqliteDatabase;

#[derive(Debug, Clone)]
pub struct SqliteHandle;

impl SqliteHandle {
    pub fn new() -> Self {
        Self
    }
}

impl DatabaseAdapter for PostgresDatabase {
    fn backend(&self) -> &'static str {
        "postgres"
    }

    fn connect(&self) -> Result<DatabaseConnection, DbError> {
        Err(DbError::NotImplemented(
            "postgres connections are not wired yet",
        ))
    }
}

impl DatabaseAdapter for SqliteDatabase {
    fn backend(&self) -> &'static str {
        "sqlite"
    }

    fn connect(&self) -> Result<DatabaseConnection, DbError> {
        println!("Opening SQLite datastore (placeholder connection)");
        Ok(DatabaseConnection::Sqlite(SqliteHandle::new()))
    }
}
