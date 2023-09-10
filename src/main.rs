extern crate dotenv;

use dotenv::dotenv;
use futures::stream::StreamExt;
use log::{info, warn};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use std::error::Error;
use std::time::Duration;

mod config;
mod kafka;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::builder()
        .format_timestamp_millis()
        .format_module_path(false)
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = config::fetch().expect("Error loading environment variables");
    let headers = config.clone().fetch_headers().expect("Error parsing headers");

    info!("Creating consumer and connecting to Kafka");
    let consumer: StreamConsumer = config.kafka_config()
        .create()
        .expect("Consumer creation failed");

    // TODO: single function to fetch topic length and current offset
    // with short lived consumer
    let metadata = consumer
        .fetch_metadata(Some("users"), Duration::from_secs(5))
        .expect("Failed to fetch metadata"); 

    let length = metadata.topics()
        .iter()
        .find(|t| t.name() == "users")
        .map(|t| kafka::fetch_topic_length(&consumer, &t))
        .unwrap_or(0);
    log::info!("Topic Length: {}", length);

    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");

    let m = consumer.recv().await;
    log::info!("Received message offset: {}", m.unwrap().offset());
    
    loop {
        let batch_size: usize = 100;
        // Pass config and headers as references
        let config_ref = &config;
        let headers_ref = &headers;
    
        let c: Vec<String> = consumer
            .stream()
            .take_while(|m| futures::future::ready(
                match m {
                    Ok(m) => m.offset() + (batch_size as i64) <= length,
                    Err(_) => false
                }
            ))
            .take(batch_size)
            .map(kafka::decode_kafka_message)
            .filter_map(|result| async {
                match result {
                    Ok(message) => Some(message),
                    Err(e) => {
                        warn!("Error while processing message: {}", e);
                        None   
                    }, 
                }
            })
            .collect::<Vec<_>>()
            .await;

        log::info!("Received {} messages", c.len());
        let client = reqwest::Client::new();
        let _res = client
            .post(&config_ref.http_target.clone())
            .json(&c)
            .headers(headers_ref.clone())
            .send()
            .await
            .expect("error sending request");
        // TODO: Re-add commit after testing
    
        if c.len() < batch_size {
            info!("Less than $BATCH_SIZE messages remaining, processing in real-time");
            while let Some(message_result) = consumer.stream().next().await {
                match kafka::decode_kafka_message(message_result) {
                    Ok(message) => {
                        log::info!("Received message: {}", message);
                        let client = reqwest::Client::new();
                        let _res = client
                            .post(&config_ref.http_target.clone())
                            .json(&[message])
                            .headers(headers_ref.clone())
                            .send()
                            .await
                            .expect("error sending request");
                    },
                    Err(e) => {
                        warn!("Error while processing message: {}", e);
                    }   
                }
            }
        }
    }
}
