use super::MessageSink;
use async_trait::async_trait;
use std::error::Error;

pub struct PostgresSink {}

pub struct PostgresConfig {
    pub postgres_host: String,
    pub postgres_port: String,
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_db: String,
    pub postgres_table: String,
}
pub(crate) fn new_instance(config: PostgresConfig) -> Result<Box<dyn MessageSink>, Box<dyn Error>> {
    todo!()
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
