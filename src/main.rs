extern crate dotenv;

use dotenv::dotenv;
use futures::stream::{self, StreamExt};
use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, BorrowedMessage};
use std::error::Error;

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

    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");
    
    
    loop {
        let config = config.clone();
        let headers = headers.clone();

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
            .post(&config.http_target.clone())
            .json(&c)
            .headers(headers)
            .send()
            .await
            .expect("error sending request");
    
        if c.len() <= 100 {
            info!("Less than $BATCH_SIZE messages remaining, processing in real-time");
            while let Some(message_result) = consumer.stream().next().await {
                match message_result {
                    Ok(borrowed_msg) => {
                        let payload = borrowed_msg.payload().unwrap();
                        let payload_str = String::from_utf8(payload.to_vec()).unwrap_or_else(|_| "Invalid UTF-8".to_string());
    
                        // Process the individual message
                        println!("Received: {}", payload_str);
                    }
                    Err(err) => {
                        println!("Error: {}", err);
                    }
                }
            }
        }
    }


}
