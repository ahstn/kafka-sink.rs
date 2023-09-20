pub(crate) mod http;
pub(crate) mod postgres;

use async_trait::async_trait;
use std::any::Any;
use std::error::Error;

#[async_trait]
pub trait MessageSink {
    async fn send_batch(&self, messages: &Vec<String>) -> Result<(), Box<dyn Error>>;
    async fn send(&self, message: &String) -> Result<(), Box<dyn Error>>;
}
