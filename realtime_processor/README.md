# Realtime Processor Project
**Realtime Processor Project** goal is to generate an engine using tecnologies like _python_, _kafka_, _spark streaming_ and _avro_. The architecture is to produce a message to Kafka using python and consume this message in real time using apache spark.

### File
Contains all files of project

customer.avsc: Avro schema containing information about a hypothetical customer.
   * Customer has the fields:
      * Customer code: Int 
      * Agency: String
      * Operation value: Double 
      * Operation type (Deposit or withdraw): int -> 0 for deposit, 1 for withdraw
      * Date: String -> format yyyy-MM-dd HH:mm:ss
      * Account Balance: Double

config.yaml: All generic configuration about the project
    * kafka_server: Kafka host
    * kafka_topic: Name of the topic
    * avro_schema: Relative location of avro schema file: file/input/customer.avsc

### Service
All services of project

avro: Manages all avro read and write, with serialization.
yaml: Manages all yaml read and write.
MySQL: Stores all processed data
Kafka: Responsible for producing the data
Spark: Responsible for receiving and processing the data

### Source codes
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

#### rule_service
Spark streaming source code responsible for streaming a kafka topic.
* Configurate env variables.
* Starts a spark session.
* Starts a stream to kafka server.
* Read stream of a topic.
* Data enrichment.


### Technologies
#### Kafka
In addition to kafka consuming the data, it was responsible for producing them using the python language.

* Produces the data
* It's the trigger of the whole flow
* Responsible for forwarding the data to the topic
* Collect the data made by python

#### Spark
* Responsible for receiving messages from Kafka
* Decode the message in AVRO format
* Enrichment of received files
* Stream processing
* Decision making

### Python
* Language used to develop the tools
* PySpark

### MySQL
* DBMS chosen for the project
* Customer data storage
* Exchange of information with Spark

### YAML
* Settings file
* Responsible for passing the parameters to the applications

### JSON
* Raw customer data
* Used to start processing and enriching data

### AVRO
* Produces data in binary formats
* Responsible for data collection
* Sending data to Kafka

### Rule Business

The business rule addressed is referring to a flow in which it classifies customers in a bank account according to their bank movements, being withdrawal, loan or renegotiation, when performing one of these movements the streaming system reacts with some operations such as unavailable balance, personal loan campaign and debt renegotiation

# Diagram

![Motor_Enriquecimento (3)](https://user-images.githubusercontent.com/66540657/176736058-c21b9862-e482-4b8d-9ec7-a790f0b94673.png)

# Decision Flowchart

![Motor_Enriquecimento (2)](https://user-images.githubusercontent.com/66540657/176736538-7ce24820-0080-4352-8e33-c309cd51d654.png)
