use std::error::Error;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::str::FromStr;
use async_trait::async_trait;
use futures_util::task::Spawn;

use super::MessageSink;

pub struct HttpSink {
    client: Client,
    http_target: String,
    http_headers: HeaderMap
}

// Configuration specific to HttpSink
pub struct HttpConfig {
    pub http_target: String,
    pub http_headers: String,
}

fn fetch_headers(s: String) -> Result<HeaderMap, Box<dyn Error>> {
    let mut headers_map: HeaderMap = HeaderMap::new();
    s.split(";").for_each(|header| {
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
pub(crate) fn new_instance(config: HttpConfig) -> Result<Box<dyn MessageSink>, Box<dyn Error>> {
  let headers = fetch_headers(config.http_headers).unwrap();
  let client = Client::new();

  Ok(Box::new(HttpSink { client, http_target: config.http_target, http_headers: headers.clone() }))
}

#[async_trait]
impl MessageSink for HttpSink {
    async fn send_batch(&self, messages: &Vec<String>) -> Result<(), Box<dyn Error>> {
        // Use self.client to send messages to self.http_target
        // Note: Actual implementation would involve using `reqwest::Client` to send messages.
        let res = self.client
            .post(&self.http_target)
            .json(messages)
            .headers(self.http_headers.clone())
            .send().await?;

        if res.status().is_success() {
            Ok(())
        } else {
            Err(Box::try_from("Error sending batch").unwrap())
        }
    }

    async fn send(&self, message: &String) -> Result<(), Box<dyn Error>> {
        // Use self.client to send message to self.http_target
        // Note: Actual implementation would involve using `reqwest::Client` to send the message.
        let res = self.client
            .post(&self.http_target)
            .json(message)
            .headers(self.http_headers.clone())
            .send().await?;

        if res.status().is_success() {
            Ok(())
        } else {
            Err(Box::try_from("Error sending batch").unwrap())
        }
    }
}