use anyhow::Context;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use log::info;
use tokio_postgres::NoTls;

use crate::config::PostgresSettings;

/// Split SQL into statements, respecting dollar-quoted strings.
/// This handles PostgreSQL function definitions that use $$ ... $$ blocks.
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut in_dollar_quote = false;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Check for dollar-quote start/end ($$)
        if i + 1 < chars.len() && chars[i] == '$' && chars[i + 1] == '$' {
            in_dollar_quote = !in_dollar_quote;
            i += 2;
            continue;
        }

        // Only treat semicolon as statement separator when not inside dollar-quote
        if chars[i] == ';' && !in_dollar_quote {
            let stmt = &sql[start..i];
            if !stmt.trim().is_empty() {
                statements.push(stmt);
            }
            start = i + 1;
        }
        i += 1;
    }

    // Handle last statement (if no trailing semicolon)
    if start < sql.len() {
        let stmt = &sql[start..];
        if !stmt.trim().is_empty() {
            statements.push(stmt);
        }
    }

    statements
}

/// PostgreSQL client with connection pooling.
///
/// Provides async database operations for relational data including
/// chains, tokens, pools, and sync checkpoints. Uses `deadpool-postgres`
/// for efficient connection management.
#[derive(Clone)]
pub struct PostgresClient {
    pub pool: Pool,
}

impl PostgresClient {
    pub async fn new(settings: PostgresSettings) -> anyhow::Result<Self> {
        info!("Connecting to PostgreSQL");

        let mut retries = 0;
        let max_retries = 3;
        #[allow(unused_assignments)]
        let mut last_error: Option<anyhow::Error> = None;

        loop {
            let mut pg_config = tokio_postgres::Config::new();
            pg_config
                .host(&settings.host)
                .port(settings.port)
                .user(&settings.user)
                .password(&settings.password)
                .dbname(&settings.database);

            let mgr_config = ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            };

            let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
            let pool = Pool::builder(mgr)
                .max_size(settings.pool_size)
                .build()
                .context("Failed to create PostgreSQL connection pool")?;

            // Test the connection
            match pool.get().await {
                Ok(_conn) => {
                    info!("Successfully connected to PostgreSQL");
                    return Ok(Self {
                        pool,
                    });
                },
                Err(e) => {
                    let error_msg = e.to_string();
                    last_error = Some(anyhow::anyhow!("{}", error_msg));
                    retries += 1;

                    if retries >= max_retries {
                        break;
                    }

                    let delay = std::time::Duration::from_millis(100 * 2_u64.pow(retries));
                    log::warn!(
                        "Failed to connect to PostgreSQL (attempt {}/{}), retrying in {:?}...",
                        retries,
                        max_retries,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                },
            }
        }

        Err(anyhow::anyhow!(
            "Failed to connect to PostgreSQL after {} attempts: {}",
            max_retries,
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "Unknown error".to_string())
        ))
    }

    /// Health check - verify connection is still alive
    pub async fn health_check(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .query_one("SELECT 1", &[])
            .await
            .context("PostgreSQL health check failed")?;
        Ok(())
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        info!("Running PostgreSQL migrations");
        let client = self.pool.get().await?;

        let schema = tokio::fs::read_to_string("schema/postgres.sql")
            .await
            .context("Failed to read schema/postgres.sql")?;

        // Split SQL statements properly, respecting dollar-quoted strings (e.g., function bodies)
        for stmt in split_sql_statements(&schema) {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            client
                .execute(stmt, &[])
                .await
                .with_context(|| format!("Failed to execute migration statement: {}", stmt))?;
        }

        info!("PostgreSQL schema applied successfully");

        // Check if inserts file exists and execute it
        if let Ok(inserts) = tokio::fs::read_to_string("schema/inserts_postgres.sql").await {
            info!("Running PostgreSQL data seeding");
            for stmt in split_sql_statements(&inserts) {
                let stmt = stmt.trim();
                if stmt.is_empty() {
                    continue;
                }
                client
                    .execute(stmt, &[])
                    .await
                    .with_context(|| format!("Failed to execute insert statement: {}", stmt))?;
            }
        }

        info!("PostgreSQL migrations completed successfully");
        Ok(())
    }
}
