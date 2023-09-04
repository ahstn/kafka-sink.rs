extern crate dotenv;

use dotenv::dotenv;
use futures::TryStreamExt;
use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tokio::time::Duration;

mod config;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::builder()
        .format_timestamp_millis()
        .format_module_path(false)
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = config::FetchConfig().expect("Error loading environment variables");

    info!("Creating consumer and connecting to Kafka");
    let kafka_config: ClientConfig = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", "kafka.rs")
        .set("auto.offset.reset", "smallest")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", &config.sasl_mechanism)
        .set("sasl.username", &config.sasl_username)
        .set("sasl.password", &config.sasl_password)
        .to_owned();

    let consumer: StreamConsumer = kafka_config
        .create()
        .expect("Error creating Kafka Consumer");

    consumer
        .subscribe(&[&config.kafka_topic])
        .expect("Can't subscribe to specified topics");

    let max_batch_size = 10;
    let batch_timeout = Duration::from_secs(5);

    log::info!("Consuming messages from Kafka");
    loop {
        // Reinitialize the variables within the loop to solve the 'moved value' problem.
        let batch = Vec::new();
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let message_stream = consumer.stream();

        // Using `async move` to take ownership of `message` and avoid lifetime issues.
        let try_for_each_result = message_stream
            .try_for_each(|message| {
                let mut batch = batch.clone(); // Clone the batch to keep the outer one untouched
                let consumer = &consumer; // Borrow the consumer to avoid moving it
                async move {
                    batch.push(message.detach());

                    if batch.len() >= max_batch_size {
                        process_and_commit_batch(consumer, &mut batch)
                            .await
                            .expect("batch error");
                        batch.clear();
                    }

                    if let Err(_) =
                        tokio::time::timeout(batch_timeout, tokio::task::yield_now()).await
                    {
                        if !batch.is_empty() {
                            process_and_commit_batch(consumer, &mut batch)
                                .await
                                .expect("batch error");
                            batch.clear();
                        }
                    }

                    Ok(())
                }
            })
            .await;

        match try_for_each_result {
            Ok(_) => {} // proceed
            Err(err) => eprintln!("Stream processing failed: {}", err),
        }
    }
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
