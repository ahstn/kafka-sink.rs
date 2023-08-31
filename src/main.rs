extern crate dotenv;

use dotenv::dotenv;
use futures_util::stream::StreamExt;
use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use std::str::FromStr;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    env_logger::builder()
        .format_timestamp_millis()
        .format_module_path(false)
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = get_env_vars()?;
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

    info!("Starting consumer");
    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                let client = reqwest::Client::new();
                let res = client
                    .post(&config.http_target)
                    .json(&serde_json::from_str::<serde_json::Value>(payload)?)
                    .headers(headers.clone())
                    .send()
                    .await?;

                if res.status().is_success() {
                    info!("Successfully sent payload: {}", payload[0..15].to_string());
                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                } else {
                    error!(
                        "Failed to send payload: {} {:?}",
                        res.status(),
                        res.text().await?
                    );
                }
            }
        };
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
