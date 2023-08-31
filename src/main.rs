extern crate dotenv;

use dotenv::dotenv;
use futures_util::stream::StreamExt;
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
    println!("Hello, world!");
    dotenv().ok();
    let config = get_env_vars()?;

    let headers: HeaderMap =
        generate_headers_map(&config.http_headers).expect("Failed to parse headers");

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

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        eprintln!("Error while deserializing message payload: {:?}", e);
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
                    println!("Successfully sent payload: {}", payload[0..15].to_string());
                } else {
                    println!(
                        "Failed to send payload: {} {:?}",
                        res.status(),
                        res.text().await?
                    );
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
