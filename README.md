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

## What is Apache Kafka and how it works
### What is Apache Kafka
- Apache Kafka is distributed pub-subs messaging systems
- Message - sequence of bytes
### Broker
- Brokers receive, store messages and serve producers and subscribers
- Can be multiple producers/consumers
### Broker cluster
- Cluster - multiple servers (from a few to 1000s)
- All produced messages will be spread among different brokers
- Every broker store part of the messages

### Zookeeper
- Maintains list of active brokers
- Manages configuration of the topics and partitions
- Elects controller
### Zookeeper ensemble
- Zookeeper cluster 
- Must be quorum - minimum number of working Zookeeper. If less Zookeeper server are live the whole Kafka custer is considered down. 
- Quorum should be (n+1)/2 where n is a number of Zookeeper servers
- Qty of Zookeeper server shoulf be odd (1, 3, 5..)

### Multiple Kafka clusters
- Each cluster is a separate entity
- It is possible to sync clusters with one another
- Clusters in different regions can work independently (without sync)
### Default ports
 - Zookeeper localhost:2181
 - Kafka srver localhost:9092
 - If you want to run multiple zookeepers/brokers you need ot run them on the different ports
 - If Zookeepers and brokers are running on different computers, there is no need to change ports
 - If Brokers should be publicaly accessible you need to adjust "advertised.listeners" property in Broker config
 
### Kafka Topic
- Messages are stored on a topic
- Topic has own name uniqui in a cluster
- Every message has a specific number called offset
- Every number and every record is immutible
- Every new message is appended to the end
- Broker may remove all records when they are expired
- Default log retention period is 168 h (7days)

### Message structure
- Timestamp
- Offset number (unique across partition)
- Key (optional) - labels a message, created by producers, helps to put a message onto a specific partition
- Value (sequence of bytes)

### Topics and Partitions
- One Topic may exist on 1 or more Brokers
- This is done with partitions
- Topics are composed of partitions which are located on one or multiple Brokers
- This improves efficiency 

### Spreading messages across partitions
- Partitions increase performance, fault tolerance
- Messages are stored in folders <TOPIC_NAME>-<PARTITION_NO>
### Partition Leader and Followers
- When a Partition is broken, all messages on that Partition are lost, unless there is the Replication
- Producers/Consumers are handled by Leader Partition. Follower Partitions (replicas) on the other Brokers only receives messages from the Leader.
- When the Leader breaks, one of the Followers becomes a new Leader
- Replication Factor is configured on a Topic. Factor = 3 is recommended.
### Controller and it's reponsibilities
- One of the brokers serves as the controller, which os resposible for managing the states of partitions and replicas and for performing administrative tasks like reassigning partitions 
- Zookeeper selects the controller
### How Producers write messages to the topic
- Messages are saved on different Partitions. Target Partition is selected according Round Robin logic
- We can decide to which particular Partition we want to send our messages using **Key** 

### How Consumers read messages from the topic
- We can consume messages form one , all or several partitions
- We can read from the beginning or some offset or a current state
- Can be multiple consumers reading from one topic. They will belong to some consumer group. Only one consumer in a group reads the messages. 
- Reading messages by the consumers belonging to one group does not affect consumers belonging to another group 