use log::{error, warn};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::metadata::MetadataTopic;
use std::error::Error;
use std::time::Duration;

/**
 * Helper function to reduce the boilerplate in decoding a kafka message.
 * We don't want to panic if we can't decode a message, so we return an empty string instead.
 */
pub fn decode_message(msg_result: Result<BorrowedMessage, KafkaError>) -> String {
    match msg_result {
        Ok(borrowed_msg) => {
            let payload = match borrowed_msg.payload_view::<str>() {
                None => "",
                Some(Ok(s)) => s,
                Some(Err(e)) => {
                    warn!("Error while deserializing message payload: {:?}", e);
                    ""
                }
            };
            String::from(payload)
        }
        Err(err) => {
            warn!("Error while receiving from Kafka: {:?}", err);
            String::from("")
        }
    }
}

pub(crate) async fn topic_length(consumer: &StreamConsumer) -> i64 {
    let metadata = consumer
        .fetch_metadata(Some("users"), Duration::from_secs(5))
        .expect("Failed to fetch metadata");

    metadata
        .topics()
        .iter()
        .find(|t| t.name() == "users")
        .map(|t| sum_partitions_watermarks(consumer, t))
        .unwrap_or(0)
}

pub fn sum_partitions_watermarks(c: &StreamConsumer, t: &MetadataTopic) -> i64 {
    return t.partitions().iter().fold(0, |acc, p| {
        let (low, high) = c
            .fetch_watermarks(t.name(), p.id(), Duration::from_secs(1))
            .unwrap_or((0, 0));
        acc + (high - low)
    });
}
