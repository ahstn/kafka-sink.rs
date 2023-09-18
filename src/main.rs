extern crate dotenv;

use crate::config::Config;
use crate::sink::MessageSink;
use config::SinkType;
use dotenv::dotenv;
use futures::stream::StreamExt;
use log::{info, warn};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use std::error::Error;

mod config;
mod kafka;
mod sink;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::builder()
        .format_timestamp_millis()
        .format_module_path(false)
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = config::fetch().expect("Error loading environment variables");

    info!("Creating consumer and connecting to Kafka");
    let consumer: StreamConsumer = config
        .kafka_config()
        .create()
        .expect("Consumer creation failed");

    let length = kafka::topic_length(&consumer).await;
    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");

    let m = consumer.recv().await;
    log::info!(
        "Topic Length: {}, Message offset: {}",
        length,
        m.unwrap().offset()
    );

    let sink = determine_sink(config);
    loop {
        let batch_size: usize = 100;
        let c: Vec<String> = consumer
            .stream()
            .take_while(|m| {
                futures::future::ready(match m {
                    Ok(m) => m.offset() + (batch_size as i64) <= length,
                    Err(_) => false,
                })
            })
            .take(batch_size)
            .map(kafka::decode_kafka_message)
            .filter_map(|result| async {
                match result {
                    Ok(message) => Some(message),
                    Err(e) => {
                        warn!("Error while processing message: {}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>()
            .await;

        log::info!("Received {} messages", c.len());
        sink.send_batch(&c).await.expect("Error sending batch");
        // TODO: re-add commit after testing

        if c.len() < batch_size {
            info!("Messages remaining less than $BATCH_SIZE, processing in real-time");
            while let Some(message_result) = consumer.stream().next().await {
                match kafka::decode_kafka_message(message_result) {
                    Ok(message) => {
                        log::info!("Received message: {}", message);
                        sink.send(&message).await.unwrap_or({
                            warn!("Error sending message to sink");
                        });
                    }
                    Err(e) => {
                        warn!("Error while processing message: {}", e);
                    }
                }
            }
        }
    }
}

fn determine_sink(config: Config) -> Box<dyn MessageSink> {
    match config.sink_type {
        SinkType::HTTP => {
            let sink_config = sink::http::HttpConfig {
                http_target: config.http_target.clone(),
                http_headers: config.http_headers.clone(),
            };
            sink::http::new_instance(sink_config).expect("Error creating sink")
        }
        SinkType::Postgres => {
            let sink_config = sink::postgres::PostgresConfig {
                postgres_host: config.postgres_host.clone(),
                postgres_port: config.postgres_port.clone(),
                postgres_user: config.postgres_user.clone(),
                postgres_password: config.postgres_password.clone(),
                postgres_db: config.postgres_db.clone(),
                postgres_table: config.postgres_table.clone(),
            };
            sink::postgres::new_instance(sink_config).expect("Error creating sink")
        }
    }
}
