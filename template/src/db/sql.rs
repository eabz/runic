use super::DbError;
use crate::config::RunicConfig;
use diesel::{
    connection::SimpleConnection,
    pg::PgConnection,
    r2d2::{ConnectionManager, Pool},
    sqlite::SqliteConnection,
};
use postgres::{Client, NoTls};
use postgres::error::SqlState;
use std::{fs, path::Path};
use url::Url;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type SqlitePool = Pool<ConnectionManager<SqliteConnection>>;

pub fn connect_postgres(config: &RunicConfig) -> Result<PgPool, DbError> {
    let base_uri = config.database.uri.trim();
    if base_uri.is_empty() {
        return Err(DbError::Configuration(
            "database.uri must be set for postgres backends".to_owned(),
        ));
    }

    let (admin_url, database_url) = build_postgres_urls(base_uri)?;
    ensure_postgres_database(&admin_url)?;

    let manager = ConnectionManager::<PgConnection>::new(database_url.clone());
    let pool = Pool::builder()
        .build(manager)
        .map_err(|err| DbError::Initialization(format!("Failed to build Postgres pool: {err}")))?;

    run_sql_migrations_postgres(&pool)?;

    Ok(pool)
}

pub fn connect_sqlite(config: &RunicConfig) -> Result<SqlitePool, DbError> {
    let path = config.database.uri.trim();
    let db_path = if path.is_empty() {
        Path::new("db.sqlite").to_path_buf()
    } else {
        Path::new(path).to_path_buf()
    };

    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| {
                DbError::Initialization(format!(
                    "Failed to create SQLite directories: {err}"
                ))
            })?;
        }
    }

    let manager = ConnectionManager::<SqliteConnection>::new(
        db_path
            .to_str()
            .ok_or_else(|| {
                DbError::Configuration(
                    "SQLite path contains invalid UTF-8 characters".to_owned(),
                )
            })?
            .to_string(),
    );

    let pool = Pool::builder()
        .build(manager)
        .map_err(|err| DbError::Initialization(format!("Failed to build SQLite pool: {err}")))?;

    run_sql_migrations_sqlite(&pool)?;

    Ok(pool)
}

fn build_postgres_urls(base: &str) -> Result<(String, String), DbError> {
    let mut admin_url = Url::parse(base).map_err(|err| {
        DbError::Configuration(format!("Invalid Postgres URI `{base}`: {err}"))
    })?;
    admin_url.set_path("/postgres");

    let mut database_url = admin_url.clone();
    database_url.set_path("/runic_indexer");

    Ok((admin_url.to_string(), database_url.to_string()))
}

fn ensure_postgres_database(admin_url: &str) -> Result<(), DbError> {
    let mut client = Client::connect(admin_url, NoTls).map_err(|err| {
        DbError::Initialization(format!(
            "Failed to connect to Postgres admin database: {err}"
        ))
    })?;

    match client.batch_execute("CREATE DATABASE runic_indexer") {
        Ok(_) => Ok(()),
        Err(err) => {
            if err.code() == Some(&SqlState::DUPLICATE_DATABASE) {
                Ok(())
            } else {
                Err(DbError::Initialization(format!(
                    "Failed to create database `runic_indexer`: {err}"
                )))
            }
        }
    }
}

fn run_sql_migrations_postgres(pool: &PgPool) -> Result<(), DbError> {
    let sql = read_sql_file("migrations/postgres.sql")?;
    let mut conn = pool.get().map_err(|err| {
        DbError::Initialization(format!(
            "Failed to obtain Postgres connection for migrations: {err}"
        ))
    })?;
    apply_sql_statements(&mut conn, &sql)
}

fn run_sql_migrations_sqlite(pool: &SqlitePool) -> Result<(), DbError> {
    let sql = read_sql_file("migrations/sqlite.sql")?;
    let mut conn = pool.get().map_err(|err| {
        DbError::Initialization(format!(
            "Failed to obtain SQLite connection for migrations: {err}"
        ))
    })?;
    conn.batch_execute("PRAGMA foreign_keys=ON;")
        .map_err(|err| {
            DbError::Migration(format!(
                "Failed to enable SQLite foreign keys: {err}"
            ))
        })?;
    apply_sql_statements(&mut conn, &sql)
}

fn read_sql_file(path: &str) -> Result<String, DbError> {
    fs::read_to_string(path)
        .map_err(|err| DbError::Migration(format!("Failed to read `{path}`: {err}")))
}

fn apply_sql_statements<C>(conn: &mut C, sql: &str) -> Result<(), DbError>
where
    C: SimpleConnection,
{
    for statement in sql.split(';').map(str::trim) {
        if statement.is_empty() {
            continue;
        }
        conn.batch_execute(statement).map_err(|err| {
            DbError::Migration(format!("Failed to execute migration `{statement}`: {err}"))
        })?;
    }
    Ok(())
}
