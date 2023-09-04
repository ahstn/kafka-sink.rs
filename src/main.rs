extern crate dotenv;

use dotenv::dotenv;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::timeout;
use rdkafka::TopicPartitionList;
use rdkafka::Offset;


#[derive(Deserialize, Debug)]
struct Config {
    kafka_brokers: String,
    kafka_topic: String,
    http_target: String,
    http_headers: String,
    sasl_mechanism: String,
    sasl_username: String,
    sasl_password: String,
}

fn get_env_vars() -> Result<Config, Box<dyn std::error::Error>> {
    match envy::from_env::<Config>() {
        Ok(config) => Ok(config),
        Err(err) => Err(Box::new(err)),
    }
}

fn generate_headers_map(headers: &str) -> Result<HeaderMap, Box<dyn std::error::Error>> {
    let mut headers_map: HeaderMap = HeaderMap::new();
    headers.split(";").for_each(|header| {
        let parts: Vec<&str> = header.split(": ").collect();
        if parts.len() == 2 {
            headers_map.insert(
                HeaderName::from_str(&parts[0]).unwrap(),
                HeaderValue::from_str(&parts[1]).unwrap(),
            );
        }
    });
    Ok(headers_map)
}

async fn process_and_commit_batch(
    consumer: &StreamConsumer,
    batch: &mut Vec<rdkafka::message::OwnedMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Replace this with your actual batch processing and sending logic
    println!("Processing batch of size {}", batch.len());

    // Create a TopicPartitionList to hold the offsets to commit
    let mut tpl = TopicPartitionList::new();
    for msg in batch.iter() {
        let topic = msg.topic();
        let partition = msg.partition();
        let next_offset = Offset::from_raw(msg.offset() + 1);
        tpl.add_partition_offset(topic, partition, next_offset)?;
    }

    consumer.commit(&tpl, CommitMode::Async)?;

    batch.clear(); // Clear the batch
    Ok(())
}


#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::builder()
        .format_timestamp_millis()
        .format_module_path(false)
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = get_env_vars().expect("Error loading environment variables");
    let headers: HeaderMap =
        generate_headers_map(&config.http_headers).expect("Failed to parse headers");

    info!("Creating consumer and connecting to Kafka");
    print!("Hello");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", "my_group")
        .set("auto.offset.reset", "smallest")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", &config.sasl_mechanism)
        .set("sasl.username", &config.sasl_username)
        .set("sasl.password", &config.sasl_password)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");

    let max_batch_size = 10;
    let batch_timeout = Duration::from_secs(5);

    loop {
        // Reinitialize the variables within the loop to solve the 'moved value' problem.
        let batch = Vec::new();
        let message_stream = consumer.stream();

        // Using `async move` to take ownership of `message` and avoid lifetime issues.
        let try_for_each_result = message_stream.try_for_each(|message| {
            let mut batch = batch.clone();  // Clone the batch to keep the outer one untouched
            let consumer = &consumer; // Borrow the consumer to avoid moving it
            async move {
                batch.push(message.detach());

                if batch.len() >= max_batch_size {
                    process_and_commit_batch(consumer, &mut batch).await.expect("batch error");
                    batch.clear();
                }

                if let Err(_) = tokio::time::timeout(batch_timeout, tokio::task::yield_now()).await {
                    if !batch.is_empty() {
                        process_and_commit_batch(consumer, &mut batch).await.expect("batch error");
                        batch.clear();
                    }
                }
                
                Ok(())
            }
        }).await;

        match try_for_each_result {
            Ok(_) => {} // proceed
            Err(err) => eprintln!("Stream processing failed: {}", err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn test_missing_env_vars() {
        env::remove_var("KAFKA_BROKERS");
        env::remove_var("KAFKA_TOPIC");
        env::remove_var("HTTP_TARGET");

        let env_result = get_env_vars();
        assert!(
            env_result.is_err(),
            "Expected error due to missing environment variables"
        );
    }

    #[test]
    fn test_valid_env_vars() {
        env::set_var("KAFKA_BROKERS", "localhost:9092");
        env::set_var("KAFKA_TOPIC", "my_topic");
        env::set_var("SASL_MECHANISM", "PLAIN");
        env::set_var("SASL_USERNAME", "my_user");
        env::set_var("SASL_PASSWORD", "my_password");
        env::set_var("HTTP_TARGET", "http://localhost:3000");
        env::set_var(
            "HTTP_HEADERS",
            "Authorization: Bearer xyz;Another-Header: value",
        );

        let env_result = get_env_vars();
        assert!(
            env_result.is_ok(),
            "Expected successful environment variable loading"
        );

        let config = env_result.unwrap();
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.kafka_topic, "my_topic");
        assert_eq!(config.http_target, "http://localhost:3000");
        // ...
    }
}
