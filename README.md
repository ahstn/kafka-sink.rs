# Kafka Sink

## Development 

Using [Materialize - Datagen | Github], we generate test data in a docker compose setup. By default 500 records are sent to a `users` topic when you run `docker-compose up`.

To check the topics with a UI, open Redpanda Console: `open localhost:8080`

[data generator | confluent hub]: https://github.com/MaterializeInc/datagen
