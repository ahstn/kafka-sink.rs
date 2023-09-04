use serde::Deserialize;
extern crate dotenv;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub http_target: String,
    pub http_headers: String,
    pub sasl_mechanism: String,
    pub sasl_username: String,
    pub sasl_password: String,
}

pub fn FetchConfig() -> Result<Config, Box<dyn std::error::Error>> {
    match envy::from_env::<Config>() {
        Ok(config) => Ok(config),
        Err(err) => Err(Box::new(err)),
    }
}
