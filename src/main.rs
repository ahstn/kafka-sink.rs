extern crate dotenv;

use dotenv::dotenv;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, BorrowedMessage};
use rdkafka::util::Timeout;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::str::FromStr;
use std::time::Duration;

mod config;

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
async fn main() {
    dotenv().ok();
    env_logger::builder()
        .format_timestamp_millis()
        .format_module_path(false)
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = config::fetch().expect("Error loading environment variables");
    let headers: HeaderMap =
        generate_headers_map(&config.http_headers).expect("Failed to parse headers");

    info!("Creating consumer and connecting to Kafka");
    print!("Hello");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", "my_group")
        .set("auto.offset.reset", "smallest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");

    let c: Vec<String> = consumer
        .stream()
        .take(100) // Take only 100 messages
        .map(|msg_result| {
            match msg_result {
                Ok(borrowed_msg) => {
                    let payload = match borrowed_msg.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            error!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };
                    payload.to_string()
                }
                Err(err) => {
                    // Handle Kafka error here, possibly returning a special String marker
                    format!("Error: {}", err)
                }
            }
        })
        .collect::<Vec<_>>()
        .await;
    log::info!("Received messages {:?} length: {}", c, c.len());
    let client = reqwest::Client::new();
    let res = client
        .post(&config.http_target)
        .json(&c)
        .headers(headers)
        .send()
        .await
        .expect("error sending request");

}
