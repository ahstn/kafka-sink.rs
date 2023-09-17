use log::error;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, BorrowedMessage};
use rdkafka::metadata::MetadataTopic;
use std::error::Error;
use std::time::Duration;
use rdkafka::util::TokioRuntime;

pub fn decode_kafka_message(msg_result: Result<BorrowedMessage, KafkaError>) -> Result<String, Box<dyn Error>> {
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
            Ok(String::from(payload))
        },
        Err(err) => return Err(err.into())
    }
}

pub(crate) async fn topic_length(consumer: &StreamConsumer) -> (i64) {
    let metadata = consumer
        .fetch_metadata(Some("users"), Duration::from_secs(5))
        .expect("Failed to fetch metadata");

    metadata.topics()
        .iter()
        .find(|t| t.name() == "users")
        .map(|t| sum_partitions_watermarks(&consumer, &t))
        .unwrap_or(0)
}

pub fn sum_partitions_watermarks(c: &StreamConsumer, t: &MetadataTopic) -> i64 {
    return t.partitions()
        .iter()
        .fold(0, |acc, p| {
            let (low, high) = c.fetch_watermarks(t.name(), p.id(), Duration::from_secs(1))
                .unwrap_or((0, 0));
            acc + (high - low)
        });
}
