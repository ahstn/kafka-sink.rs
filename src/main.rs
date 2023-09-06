extern crate dotenv;

use dotenv::dotenv;
use futures::stream::{self, StreamExt};
use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, BorrowedMessage, self};
use rdkafka::metadata::{self, MetadataPartition};
use std::error::Error;
use std::option::Iter;
use std::time::Duration;

mod config;

fn decode_kafka_message(msg_result: Result<BorrowedMessage, KafkaError>) -> Result<String, Box<dyn Error>> {
    match msg_result {
        Ok(borrowed_msg) => {
            let payload = match borrowed_msg.payload_view::<str>() {
                None => "",
                Some(Ok(s)) => s,
                Some(Err(e)) => {
                    error!("Error while deserializing message payload: {:?}", e);
                    return Err(e.into());
                }
            };
            Ok(payload.to_string())
        },
        Err(err) => {
            return Err(err.into());
        }
    }
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
    let headers = config.clone().fetch_headers().expect("Error parsing headers");

    info!("Creating consumer and connecting to Kafka");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", "my_group")
        .set("auto.offset.reset", "smallest")
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(Some("users"), Duration::from_secs(5))
        .expect("Failed to fetch metadata"); 

    let length = metadata.topics()
        .iter()
        .find(|t| t.name() == "users")
        .and_then(|t| {
            let sum: i64 = t.partitions()
                .iter()
                .map(|p| {
                    let (low, high) = consumer.fetch_watermarks("users", p.id(), Duration::from_secs(1))
                        .unwrap_or((-1, -1));
                    high - low
                })
                .sum();
            Some(sum)
        })
        .unwrap_or(0);

    log::info!("Length: {}", length);

    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");
    
    loop {
        // Pass config and headers as references
        let config_ref = &config;
        let headers_ref = &headers;
    
        // Change this to `chunk` and `for_each` on all chunks?
        let c: Vec<String> = consumer
            .stream()
            .take(100) // Take only 100 messages
            .map(decode_kafka_message)
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
        let res = client
            .post(&config_ref.http_target.clone())
            .json(&c)
            .headers(headers_ref.clone())
            .send()
            .await
            .expect("error sending request");
    
        if c.len() <= 100 {
            info!("Less than $BATCH_SIZE messages remaining, processing in real-time");
            while let Some(message_result) = consumer.stream().next().await {
                match decode_kafka_message(message_result) {
                    Ok(message) => {
                        log::info!("Received message: {}", message);
                        let client = reqwest::Client::new();
                        let res = client
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

