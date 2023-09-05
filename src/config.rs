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

pub fn fetch() -> Result<Config, Box<dyn std::error::Error>> {
    match envy::from_env::<Config>() {
        Ok(config) => Ok(config),
        Err(err) => Err(Box::new(err)),
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn test_missing_env_vars() {
        env::remove_var("KAFKA_BROKERS");
        env::remove_var("KAFKA_TOPIC");
        env::remove_var("HTTP_TARGET");

        let env_result = fetch();
        assert!(
            env_result.is_err(),
            "Expected error due to missing environment variables"
        );
    }

    #[test]
    fn test_valid_env_vars() {
        env::set_var("KAFKA_BROKERS", "localhost:9092");
        env::set_var("KAFKA_TOPIC", "my_topic");
        env::set_var("SASL_MECHANISM", "PLAIN");
        env::set_var("SASL_USERNAME", "my_user");
        env::set_var("SASL_PASSWORD", "my_password");
        env::set_var("HTTP_TARGET", "http://localhost:3000");
        env::set_var(
            "HTTP_HEADERS",
            "Authorization: Bearer xyz;Another-Header: value",
        );

        let env_result = fetch();
        assert!(
            env_result.is_ok(),
            "Expected successful environment variable loading"
        );

        let config = env_result.unwrap();
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.kafka_topic, "my_topic");
        assert_eq!(config.http_target, "http://localhost:3000");
        // ...
    }
}