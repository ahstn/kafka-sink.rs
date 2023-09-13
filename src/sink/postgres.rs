use std::error::Error;
use async_trait::async_trait;
use super::MessageSink;

pub struct PostgresSink {

}

pub struct PostgresConfig {

}
fn new_instance(config: PostgresConfig) -> Result<PostgresSink, &'static str> {
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