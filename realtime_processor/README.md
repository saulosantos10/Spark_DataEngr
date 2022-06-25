# Realtime Processor Project
**Realtime Processor Project** goal is to generate an engine using tecnologies like _python_, _kafka_, _spark streaming_ and _avro_. The architecture is to produce a message to Kafka using python and consume this message in real time using apache spark.

## Source codes
#### kafka_producer.py
This source code is responsible to make the whole flow of the producer, it does the following:
* Reads the config file with generic configuration
* Creates a kafka producer
* Read a json file with all data
* Send this data serialized to a kafka topic

#### kafka_consumer.py
This source is a test to consume everything that comes into a kafka topic, it does the following:
* Reads the config file with generic configuration
* Create a kafka consumer in a topic
* Print deserialized value into console.

#### spark_consumer.py
Spark streaming source code responsible for streaming a kafka topic.
* Configurate env variables.
* Starts a spark session.
* Starts a stream to kafka server.
* Read stream of a topic.
* Data enrichment

## Service
All services of project
* avro: Manages all avro read and write, with serialization.
* yaml: Manages all yaml read and write.

## File
Contains all files of project
   * customer.avsc: Avro schema containing information about a hypothetical customer.
      * Customer has the fields:
         * Customer code: Int
         * Agency: String
         * Operation value: Double
         * Operation type (Deposit or withdraw): int -> 0 for deposit, 1 for withdraw
         * Date: String -> format yyyy-MM-dd HH:mm:ss
         * Account Balance: Double

   * config.yaml: All generic configuration about the project
      * kafka_server: Kafka host
         * kafka_topic: Name of the topic
         * avro_schema: Relative location of avro schema file: file/input/customer.avsc