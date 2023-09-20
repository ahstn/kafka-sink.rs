extern crate dotenv;

use crate::config::Config;
use crate::sink::MessageSink;
use config::SinkType;
use dotenv::dotenv;
use futures::stream::StreamExt;
use log::{debug, info, warn};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;

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
    log::info!("Topic Length: {}", length);

    let batch_size: i64 = 100;
    let sink_type = config.sink_type.to_owned();
    let sink = determine_sink(config).await;
    if length > (batch_size * 2) && sink_type != SinkType::Postgres {
        log::info!(
            "Batch size: {}, looping {} times",
            batch_size,
            (length / batch_size)
        );
        let mut i = 0;
        while i < (length / batch_size) {
            let c: Vec<String> = consumer
                .stream()
                .take(100)
                .map(kafka::decode_message)
                .collect()
                .await;

            log::info!("Received {} messages", c.len());
            sink.send_batch(&c).await.expect("Error sending batch");
            // TODO: re-add commit after testing

            log::info!("Sent {} messages", c.len());
            i = i + 1;
        }
    }

    info!("Messages remaining less than $BATCH_SIZE, processing in real-time");
    while let Some(message_result) = consumer.stream().next().await {
        let message = kafka::decode_message(message_result);
        log::info!("Received message: {}", message);
        match sink.send(&message).await {
            Ok(_) => (),
            Err(e) => {
                warn!("Error while sending message: {}", e);
            }
        };
    }
}

async fn determine_sink(config: Config) -> Box<dyn MessageSink> {
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
            sink::postgres::new_instance(sink_config)
                .await
                .expect("Error creating sink")
        }
    }
}
