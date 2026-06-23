# Learn core Apache Kafka features, create Java, Node.js, Python Producers and Consumers - Full Kafka hands-on experience!

## Installing Kafka on Windows
1. [Kafka dowloads](https://kafka.apache.org/community/downloads/)
2. Extract and put on **C:**
3. Rename the directory just to **Kafka**
4. Install JDK

### Starting on Windows
1. Edit **/config/server.properties**
  -  Configure 
```
log.dirs=C:/kafka/kafka-logs
```
2. Edit **/config/zookeeper.properties**
```
dataDir=C:/kafka/zookeeper
```
3. Run Zookeeper
```C:\kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties```
4. Run Kafka
```
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server.properties

```
## Starting Apache Zookeeper and Kafka Broker
- C:\kafka:
  - bin - starting scripts
  - config - brocker, producer, zookeeper configs
  - logs - log files
  - kafka-logs - kafka messages
  
## Creating and exploring Kafka Topic
- Default ports:
  - Zookeeper lcoalhost:2181
  - Kafka server localhost:9092
  
### Create a new Kafka topic
- run `C:\kafka\bin\windows>.\kafka-topics.bat` - info how to use this script
- run `C:\kafka\bin\windows> .\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic cities`
### What happened after creation a new topic
- There is a new folder cities-0  (partition) inside  kafka-logs
- topic consist of partitions (num.partitions=1)

### Read details about a topic
- run `C:\kafka\bin\windows> .\kafka-topics.bat --list --bootstrap-server localhost:9092` - list of the topics
- run `C:\kafka\bin\windows> .\kafka-topics.bat --describe --bootstrap-server localhost:9092 --topic cities` - info about the topic configuration
- PartitionCount: 1 - nr of partitions
- ReplicationFactor: 1 - how many time the messages are replicated (for backup)

## Producing and consuming Messages
### Send some messages
- run `C:\kafka\bin\windows> .\kafka-console-producer.bat --broker-list localhost:9092 --topic cities` 
### Consume messages
- run `.\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic cities
### Consuming messages from the beginning
- run `C:\kafka\bin\windows> .\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic cities --from-beginning`
- Kafka cluster stores messages even if they were consumed
- Same messages ,ay be read multiple times by different consumers
### Running multiple consumers
- Multiple consumers and multiple producers could exchange messages via single centralized storage point -kafka cluster
### Running multiple producers
- Producers and consumers may appear and disappear. But Kafka doesn't care about that. It's job is to store messages and receive or send them on demand.

### What was changed in the Kafka logs
- Every consumer must be part of rhe consumer group
- Every message inside of the topic has unique number called "offset"
- First message in each topic has offet 0
- Cunsumers start reading messages starting from specific offset