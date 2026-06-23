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