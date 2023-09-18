use super::MessageSink;
use async_trait::async_trait;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use std::error::Error;
use tokio_postgres::NoTls;

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
    async fn send_batch(&self, messages: &Vec<String>) -> Result<(), Box<dyn Error>> {
        // Actual logic for sending a batch of messages to Postgres database.
        todo!()
    }

    async fn send(&self, message: &String) -> Result<(), Box<dyn Error>> {
        // Actual logic for sending a single message to Postgres database.
        todo!()
    }
}
