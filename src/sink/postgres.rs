use super::MessageSink;
use async_trait::async_trait;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use tokio_postgres::NoTls;

#[derive(Clone)]
pub struct PostgresSink {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

pub struct PostgresConfig {
    pub postgres_host: String,
    pub postgres_port: String,
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_db: String,
    pub postgres_table: String,
}

impl PostgresConfig {
    pub fn tokio_postgres_config(&self) -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::new();
        config.host(&self.postgres_host);
        config.port(self.postgres_port.parse::<u16>().unwrap());
        config.user(&self.postgres_user);
        config.password(&self.postgres_password);
        config.dbname(&self.postgres_db);
        config
    }
}

pub(crate) async fn new_instance(
    config: PostgresConfig,
) -> Result<Box<dyn MessageSink>, Box<dyn Error>> {
    let manager = PostgresConnectionManager::new(config.tokio_postgres_config(), NoTls);
    let pool = Pool::builder().build(manager).await?;

    Ok(Box::new(PostgresSink { pool }))
}

#[async_trait]
impl MessageSink for PostgresSink {
    async fn send_batch(&self, _messages: &Vec<String>) -> Result<(), Box<dyn Error>> {
        // Actual logic for sending a batch of messages to Postgres database.
        todo!("Batch pre-load not yet implemented for Postgres")
    }

    async fn send(&self, message: &String) -> Result<(), Box<dyn Error>> {
        // Create hashmap from JSON payload to extract keys and values separately
        // TODO: Does values needs to be generic to allow ints?
        let deserialized: HashMap<String, Value> = serde_json::from_str(message)?;
        let keys: Vec<String> = deserialized.keys().cloned().collect();
        let values: Vec<String> = deserialized.values().map(|v| v.to_string()).collect();

        // First replace is to santise single quotes in values
        // Second replace is to replace the double quotes that serde_json(?) adds
        let query = format!(
            "INSERT INTO users ({}) VALUES({});",
            keys.join(", "),
            values.join(", ").replace('\'', "''")
        )
        .replace('"', "'");

        let conn = self.pool.get().await;
        match conn {
            Ok(_) => (),
            Err(e) => {
                log::info!("Error getting connection: {}", e);
                return Err(Box::try_from("Error getting connection").unwrap());
            }
        }

        // Execute query
        match conn.unwrap().batch_execute(&query).await {
            Ok(_) => (),
            Err(e) => {
                log::info!("Error executing query: {}", e);
                return Err(Box::try_from("Error executing query").unwrap());
            }
        };

        Ok(())
    }
}
