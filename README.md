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

## GitHub Repository and Diagrams
- [Apache Kafka Course - github](https://github.com/bstashchuk/apache-kafka-course)
- Basic commands
```bash
Basic KAFKA Commands

START ZOOKEEPER
bin/zookeeper-server-start.sh config/zookeeper.properties

START KAFKA BROKER
bin/kafka-server-start.sh config/server.properties

CREATE TOPIC
bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--replication-factor 1 \
--partitions 3 \
--topic test

LIST TOPICS
bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--list

TOPIC DETAILS
bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic test

START CONSOLE PRODUCER
bin/kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic test

START CONSOLE CONSUMER
bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic test

START CONSOLE CONSUMER AND READ MESSAGES FROM BEGINNING
bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic test \
--from-beginning

START CONSOLE CONSUMER WITH SPECIFIC CONSUMER GROUP
bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic test \
--group test \
--from-beginning

LIST CONSUMER GROUPS
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--list

CONSUMER GROUP DETAILS
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--group test \
--describe

```
## Example 1 - Topic with Multiple Partitions
### If you use remote Kafka brokers
In case you use Kafka brokers and zookeeper remotely on any of the hosting services like Amazon EC2, Google Cloud or Digitalocean and want to connect to it from your local computer (produce or consume messages) you need to do following:

1. On hosting service firewall allow remote access and open ports 2181 (Zookeeper) and 9092 (Broker)

2. In the configuration file for each broker you need to adjust  advertised.listeners and set it either to DNS name or public IP address of the server where broker is hosted.

Examples
```
advertised.listeners=PLAINTEXT://186.23.16.1:9092
advertised.listeners=PLAINTEXT://ec2-54-123-123-123.compute-1.amazonaws.com:9092
```
Don't forget to uncomment this line in the configuration file!

If you use only one broker with default configuration file server.properties, do this adjustment in it. If you use multiple brokers in the cluster with multiple configuration files, make this adjustment in each of the custom configuration files.

### Cleaning up existing Kafka installation
1. Run Zookeeper

```
C:\kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

2. Run Kafka

```
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server.properties
```

3. Create a Topic

```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --replication-factor 1  --partitions 3 --topic animals
```
4. Start console producer
```
C:\kafka> .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic animals
cat
dog 
monkey
```
5. Start console consumer

```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic animals --from-beginning
```
### How messages were spread
- Messages are not consumed in an order of adding
- Every offset number must be unique across partition 
- Partitions: animals-0, animals-1, animals-2
- Distribution of messages is not done in a specic order

### Reading from a specific partition
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --partition 1 --topic animals --from-beginning
```
### Reading from a specific offset
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic animals --partition 2 --offset 3
```
- It is not possible to read from specific offset across entire topic. You must specify partition along with offset.


### Reading details about topic and __consumer_offsers topic
```
# LIST TOPICS
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list

# __consumer_offsets - system topic
# custom topics
# animals 
# cities

# TOPIC DETAILS
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic animals

# Topic: animals  TopicId: VZRvfduITv64jANbeN-9sg PartitionCount: 3  
# ReplicationFactor: 1    Configs:
#       Topic: animals  Partition: 0    Leader: 0       Replicas: 0     Isr: 0
#       Topic: animals  Partition: 1    Leader: 0       Replicas: 0     Isr: 0
#       Topic: animals  Partition: 2    Leader: 0       Replicas: 0     Isr: 0
``` 
- segment - sing file that stores messages
- Isr - id of a broker

```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic __consumer_offsets

# Topic: __consumer_offsets       TopicId: IRdkj7p3RoavZq4pndHl_A PartitionCount: 50      # ReplicationFactor: 1    Configs: # compression.type=producer,cleanup.policy=compact,segment.bytes=104857600
        Topic: __consumer_offsets       Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 1    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 3    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 4    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 5    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 6    Leader: 0       Replicas: 0     Isr: 0
```
- __consumer_offsets - commited offsets of consumers

## Kafka Cluster with Multiple Brokers
### Creating separate configuation files for brokers
- Every broker on the same computer must have:
  - Unique port
  - Unique broker ID
  - Unique log directory
- Copy server.properties
  - We should have : server.properties, server0.properties, server1.properties, server2.properties
  
- server0.properties
```
broker.id=0

#listeners=PLAINTEXT://:9092

log.dirs=C:/kafka/kafka-logs-0

```  
- server1.properties
```
broker.id=1

listeners=PLAINTEXT://:9093

log.dirs=C:/kafka/kafka-logs-1

```  
- server2.properties
```
broker.id=2

listeners=PLAINTEXT://:9094

log.dirs=C:/kafka/kafka-logs-2

```  
### Launching three brokers
1. Run Zookeeper
```
C:\kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

2. Run Kafka

```
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server0.properties
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server1.properties
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server2.properties
```

### Getting cluster information
1. Get information from Zookeeper about active broker Ids
```
C:\kafka> .\bin\windows\zookeeper-shell.bat localhost:2181 ls /brokers/ids

# Connecting to localhost:2181

# WATCHER::

# WatchedEvent state:SyncConnected type:None path:null
# [0, 1, 2]
```
2. Get information from Zookeeper about specific broker by Id
```
C:\kafka> .\bin\windows\zookeeper-shell.bat localhost:2181 get /brokers/ids/0

# Connecting to localhost:2181

# WATCHER::

# WatchedEvent state:SyncConnected type:None path:null
# {"features":{},"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":#["PLAINTEXT://DESKTOP-MIMEIU7:9092"],"jmx_port":-1,"port":9092,"host":"DESKTOP-MIMEIU7","version":5,"timestamp":"1782722524283"}
```
### Creating multiple-partition topic in the Kafka cluster
- Creating a topic
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --create --replication-factor 1 --partitions 5 --topic cars

# Created topic cars.
```
### Producing and consuming messages in the cluster
- Start a producer
```
C:\kafka> .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092,localhost:9093,localhost:9094 --topic cars

>BMW
>Tesla
>Honda
>Audi
```
- Start a consumer
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic cars --from-beginning


# BMW
# Tesla
# Honda
# Audi
# Maserali
```

### Details about topic in the cluster
- List all topics

```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --list

# __consumer_offsets
# cars
```

- Topic details

```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --describe --topic cars

Topic: cars     TopicId: KvGZ2IoNQYGG6LdO7AwR1A PartitionCount: 5       ReplicationFactor: 1    Configs:
        Topic: cars     Partition: 0    Leader: 2       Replicas: 2     Isr: 2
        Topic: cars     Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: cars     Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: cars     Partition: 3    Leader: 2       Replicas: 2     Isr: 2
        Topic: cars     Partition: 4    Leader: 1       Replicas: 1     Isr: 1
```
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --describe --topic __consumer_offsets
Topic: __consumer_offsets       TopicId: ZPPQAr3JRkGpHNCjkjZPJg PartitionCount: 50      ReplicationFactor: 1    Configs: compression.type=producer,cleanup.policy=compact,segment.bytes=104857600
        Topic: __consumer_offsets       Partition: 0    Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 3    Leader: 2       Replicas: 2     Isr: 2
```
### Simulating broker failure in the cluster
1. Stop broker 2
2. List brokers

```
PS C:\kafka> .\bin\windows\zookeeper-shell.bat localhost:2181 ls /brokers/ids
Connecting to localhost:2181

WATCHER::

WatchedEvent state:SyncConnected type:None path:null
[0, 1]
```

- Read messages again. Messages from broker 2 are not available.
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic cars --from-beginning
```
- Describe topic cars

```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --describe --topic cars
Topic: cars     TopicId: KvGZ2IoNQYGG6LdO7AwR1A PartitionCount: 5       ReplicationFactor: 1    Configs:
        Topic: cars     Partition: 0    Leader: none    Replicas: 2     Isr: 2
        Topic: cars     Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: cars     Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: cars     Partition: 3    Leader: none    Replicas: 2     Isr: 2
        Topic: cars     Partition: 4    Leader: 1       Replicas: 1     Isr: 1
```

- Start broker 2 again
```
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server2.properties
```
## Multiple Brokers and Topic with Replication
### Prepare for nex example
- Remove Kafka and Zookeeper logs
- Copy server0.properties, server1.properties,server2.properties from git project `apache-kafka-course-master\EXAMPLES\03 Multiple Brokers and Replication\config`

### Launching brokers and creating topics with replication
- Start Zookeeper
- Start Brokers
- Verify

```
C:\kafka> .\bin\windows\zookeeper-shell.bat localhost:2181 ls /brokers/ids
```
- Create a Topic
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --create --replication-factor 3 --partitions 7 --topic months

# Created topic months.
```
- Only **Leader** broker in the partition serves producers and consumers
- List topics
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --list
```

- Describe topics
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --describe --topic months

Topic: months   TopicId: Zm_P_gFwQqibpWCBgMh7JA PartitionCount: 7       ReplicationFactor: 3    Configs: segment.bytes=1073741824
        Topic: months   Partition: 0    Leader: 2       Replicas: 2,1,0 Isr: 2,1,0
        Topic: months   Partition: 1    Leader: 1       Replicas: 1,0,2 Isr: 1,2,0
        Topic: months   Partition: 2    Leader: 0       Replicas: 0,2,1 Isr: 2,1,0
        Topic: months   Partition: 3    Leader: 2       Replicas: 2,0,1 Isr: 2,1,0
        Topic: months   Partition: 4    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0
        Topic: months   Partition: 5    Leader: 0       Replicas: 0,1,2 Isr: 1,2,0
        Topic: months   Partition: 6    Leader: 2       Replicas: 2,1,0 Isr: 2,1,0
```
- Start a consumer
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic months --from-beginning
```
- Start a producer
```
C:\kafka> .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092,localhost:9093,localhost:9094 --topic months
```
- Read from a specific partition

```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic months --partition 3 --from-beginning
```
- Read from a specific partition and offset

```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic months --partition 4 --offset 2
```

### Observing how messages were stored in the partitions on different brokers
- Contenst of any patitions are the same on every broker due to the replication
- After joining/shutdown of a broker, a new leader is elected

## Kafka Consumer Groups
- Every consumer must belong to any consumer group. If you do not specify your custom group, it will be created automatically

### Exploring default consumer groups
1. Start Zookeeper.
```
C:\kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```
2. Start a single broker

```
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server.properties
```
3. Create a topic **numbers**
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 5 --topic numbers
```
4. List the topics.
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```
5. Start a consumer without consumer group.
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic numbers
```
6. Start a producer
```
C:\kafka> .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic numbers

>1
>2
>3
```
7. List consumer groups

```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
console-consumer-35304
```
8. Run another consumer
9. List consumer groups
```
# 2 groups are created
console-consumer-23615
console-consumer-35304
```
- groups are created to reduce the load on counsumers
- single consumer may not be able to consume all produced messages at the same high rates
- messages will be spread among different consumers
10. Details of the consumer group
```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group console-consumer-23615 --describe

GROUP                  TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                           HOST            CLIENT-ID
console-consumer-23615 numbers         0          -               0               -               console-consumer-7fe332b3-5f44-4645-a83d-b9a570efeb57 /192.168.100.10 console-consumer
console-consumer-23615 numbers         3          -               0               -               console-consumer-7fe332b3-5f44-4645-a83d-b9a570efeb57 /192.168.100.10 console-consumer
console-consumer-23615 numbers         4          -               5               -               console-consumer-7fe332b3-5f44-4645-a83d-b9a570efeb57 /192.168.100.10 console-consumer
console-consumer-23615 numbers         1          -               0               -               console-consumer-7fe332b3-5f44-4645-a83d-b9a570efeb57 /192.168.100.10 console-consumer
console-consumer-23615 numbers         2          -               0               -               console-consumer-7fe332b3-5f44-4645-a83d-b9a570efeb57 /192.168.100.10 console-consumer
```
11. Stop both consumers
12. List consumer groups

```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list

console-consumer-23615
console-consumer-35304

```
- Consumer group is automatically deleted when the last commited offset for the group expires (offsets.retention.minutes - default 24 hours)

### Starting consumer in the custom consumer group
1. Start a new consume rwith a custom customer group

```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic numbers --group nums --from-beginning
```
2. List customr groups

```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
numbers-group
```
3. Read the details about the group

```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group nums --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                           HOST            CLIENT-ID
nums            numbers         0          0               0               0               console-consumer-de5d55e6-89c6-4a21-bf87-a318098d4016 /192.168.100.10 console-consumer
nums            numbers         3          0               0               0               console-consumer-de5d55e6-89c6-4a21-bf87-a318098d4016 /192.168.100.10 console-consumer
nums            numbers         4          5               5               0               console-consumer-de5d55e6-89c6-4a21-bf87-a318098d4016 /192.168.100.10 console-consumer
nums            numbers         1          0               0               0               console-consumer-de5d55e6-89c6-4a21-bf87-a318098d4016 /192.168.100.10 console-consumer
nums            numbers         2          0               0               0               console-consumer-de5d55e6-89c6-4a21-bf87-a318098d4016 /192.168.100.10 console-consumer
```
- LAG is non zero when a customem has not finished reading all the messages in the partition

### Starting a second consumer in the same consumer group

```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic numbers --group nums --from-beginning

```
- No messages are read as previous customer (from the same group) has already read everything
- Describe again:

```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group nums --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                           HOST            CLIENT-ID
nums            numbers         0          0               0               0               console-consumer-3504784a-3133-4fd8-acac-da18e5f4f862 /192.168.100.10 console-consumer
nums            numbers         1          0               0               0               console-consumer-3504784a-3133-4fd8-acac-da18e5f4f862 /192.168.100.10 console-consumer
nums            numbers         2          0               0               0               console-consumer-3504784a-3133-4fd8-acac-da18e5f4f862 /192.168.100.10 console-consumer
nums            numbers         3          0               0               0               console-consumer-b3626c0f-cd21-4598-af23-071a49884e1a /192.168.100.10 console-consumer
nums            numbers         4          6               6               0               console-consumer-b3626c0f-cd21-4598-af23-071a49884e1a /192.168.100.10 console-consumer
```

### Starting one more consumer in the same consumer group
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic numbers --group nums --from-beginning
```
- Describe again:
```
PS C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group nums --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                           HOST            CLIENT-ID
nums            numbers         0          4               4               0               console-consumer-3504784a-3133-4fd8-acac-da18e5f4f862 /192.168.100.10 console-consumer
nums            numbers         1          0               0               0               console-consumer-3504784a-3133-4fd8-acac-da18e5f4f862 /192.168.100.10 console-consumer
nums            numbers         2          0               0               0               console-consumer-b3626c0f-cd21-4598-af23-071a49884e1a /192.168.100.10 console-consumer
nums            numbers         3          0               0               0               console-consumer-b3626c0f-cd21-4598-af23-071a49884e1a /192.168.100.10 console-consumer
nums            numbers         4          6               6               0               console-consumer-bf4f0d23-75ca-4d0b-b466-a1ac7c14eb2b /192.168.100.10 console-consumer
```
- three different consumer ids

### Idle cunsumers in the group
- Run 3 more consumers
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic numbers --group nums --from-beginning
```
- Describe
```
PS C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group nums --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                           HOST            CLIENT-ID
nums            numbers         0          7               7               0               console-consumer-3504784a-3133-4fd8-acac-da18e5f4f862 /192.168.100.10 console-consumer
nums            numbers         3          0               0               0               console-consumer-9714bbee-3dc9-40cb-b292-6ec4df227cd8 /192.168.100.10 console-consumer
nums            numbers         2          0               0               0               console-consumer-783b8c08-2151-4ea1-9176-523990973dc2 /192.168.100.10 console-consumer
nums            numbers         4          6               6               0               console-consumer-b3626c0f-cd21-4598-af23-071a49884e1a /192.168.100.10 console-consumer
nums            numbers         1          0               0               0               console-consumer-6a4c9897-2980-47e3-9127-f5d4903defba /192.168.100.10 console-consumer
```
- One consumer is idle
- If thre are 3 partitions in the topic and 100 consumers in the group then 97 consumers will be idle and only 3 of 100 will be assigned to partitions

## Performance testing
- "Lagging" - messages are produced faster than consumers can cunsume them
- Use config files from: `apache-kafka-course-master\EXAMPLES\05 Performance Testing\config` 

### Starting cluster and launching basic performance test
1. Start Zookeeper and the cluster
```
C:\kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server0.properties

C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server1.properties

C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server2.properties
```
2. Create a topic

```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --replication-factor 3 --partitions 100 --topic perf

Created topic perf.
```
3. Start a consumer

```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic perf

```
4. Start producer performance test
```
C:\kafka> .\bin\windows\kafka-producer-perf-test.bat --topic perf --num-records 1000 --throughput 100 --record-size 1000 --producer-props  bootstrap.servers=localhost:9092

501 records sent, 100,1 records/sec (0,10 MB/sec), 2,7 ms avg latency, 226,0 ms max latency.
1000 records sent, 99,990001 records/sec (0,10 MB/sec), 1,88 ms avg latency, 226,00 ms max latency, 1 ms 50th, 2 ms 95th, 21 ms 99th, 226 ms 99.9th.
```
- **num-record** - how many messages in total
- **throughput** - how many msgs per sec will be produced
- **record-size** - size of every message in bytes
- **producer-props** - list of properties

### Increasing performance test parameters
1. Run performance test again
```
C:\kafka> .\bin\windows\kafka-producer-perf-test.bat --topic perf --num-records 10000 --throughput 1000 --record-size 1000 --producer-props  bootstrap.servers=localhost:9092

4995 records sent, 997,6 records/sec (0,95 MB/sec), 1,9 ms avg latency, 237,0 ms max latency.
10000 records sent, 999,500250 records/sec (0,95 MB/sec), 1,37 ms avg latency, 237,00 ms max latency, 1 ms 50th, 2 ms 95th, 17 ms 99th, 21 ms 99.9th.
```
2. Change parameters and run the test

```
C:\kafka> .\bin\windows\kafka-producer-perf-test.bat --topic perf --num-records 100000 --throughput 10000 --record-size 1000 --producer-props  bootstrap.servers=localhost:9092

49922 records sent, 9972,4 records/sec (9,51 MB/sec), 2,6 ms avg latency, 230,0 ms max latency.
50020 records sent, 10004,0 records/sec (9,54 MB/sec), 0,8 ms avg latency, 11,0 ms max latency.
100000 records sent, 9986,019573 records/sec (9,52 MB/sec), 1,66 ms avg latency, 230,00 ms max latency, 1 ms 50th, 3 ms 95th, 30 ms 99th, 39 ms 99.9th.
```

### Testing consumer performance
1. Run Kafka consumer test
```
C:\kafka> .\bin\windows\kafka-consumer-perf-test.bat --broker-list localhost:9092 --topic perf --messages 10000

start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-07-01 11:28:04:562, 2026-07-01 11:28:05:084, 9,5367, 18,2696, 10000, 19157,0881, 1782898084864, -1782898084342, -0,0000, -0,0000
```
2. Run again for more messages
```
C:\kafka> .\bin\windows\kafka-consumer-perf-test.bat --broker-list localhost:9092 --topic perf --messages 100000

start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-07-01 11:29:39:209, 2026-07-01 11:29:39:769, 95,6316, 170,7707, 100277, 179066,0714, 1782898179448, -1782898178888, -0,0000, -0,0001
```
- If consumer consumes slowly than producers produce there will be **LAG** between offsets.
### Getting non-zero LAG
1. Stop the consumer
2. Create a new topic
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --replication-factor 3 --partitions 3 --topic perf2

Created topic perf2.
```
3. Start 2 consumers in a specific group
```
C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic perf2 --group perf --from-beginning
```
4. Read details about consumer group
```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group perf --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                          HOST            CLIENT-ID
perf            perf2           0          0               0               0               consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           1          0               0               0               consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           2          0               0               0               consumer-perf-1-94c420ba-f670-49ca-8d19-4873425814d0 /192.168.100.10 consumer-perf-1
```
5. Run producer performance test
```
C:\kafka> .\bin\windows\kafka-producer-perf-test.bat --topic perf2 --num-records 1000 --throughput 10 --record-size 100000 --producer-props  bootstrap.servers=localhost:9092

52 records sent, 10,3 records/sec (0,98 MB/sec), 7,8 ms avg latency, 204,0 ms max latency.
51 records sent, 10,0 records/sec (0,95 MB/sec), 2,3 ms avg latency, 4,0 ms max latency.
51 records sent, 10,0 records/sec (0,95 MB/sec), 1,9 ms avg latency, 4,0 ms max latency.
51 records sent, 10,0 records/sec (0,95 MB/sec), 2,1 ms avg latency, 4,0 ms max latency.
```
6. Read details about consumer group a few times
```
C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group perf --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                          HOST            CLIENT-ID
perf            perf2           0          45              61              16              consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           1          39              56              17              consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           2          32              47              15              consumer-perf-1-94c420ba-f670-49ca-8d19-4873425814d0 /192.168.100.10 consumer-perf-1

C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group perf --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                          HOST            CLIENT-ID
perf            perf2           0          220             229             9               consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           1          209             218             9               consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           2          188             195             7               consumer-perf-1-94c420ba-f670-49ca-8d19-4873425814d0 /192.168.100.10 consumer-perf-1

C:\kafka> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group perf --describe

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                          HOST            CLIENT-ID
perf            perf2           0          361             361             0               consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           1          321             321             0               consumer-perf-1-33b25b0c-e26d-4b88-881d-698a63beeddf /192.168.100.10 consumer-perf-1
perf            perf2           2          318             318             0               consumer-perf-1-94c420ba-f670-49ca-8d19-4873425814d0 /192.168.100.10 consumer-perf-1
```
- LOG-END-OFFSET - CURRENT-OFFSET = LAG

## Project 1 - Java
### If you use remote Kafka brokers
In case you use Kafka brokers and zookeeper remotely on any of the hosting services like Amazon EC2, Google Cloud or Digitalocean and want to connect to it from your local computer (produce or consume messages) you need to do following:

1. On hosting service firewall allow remote access and open ports 2181 (Zookeeper) and 9092 (Broker)

2. In the configuration file for each broker you need to adjust  **advertised.listeners** and set it either to DNS name or public IP address of the server where broker is hosted.

Examples

```
advertised.listeners=PLAINTEXT://186.23.16.1:9092
advertised.listeners=PLAINTEXT://ec2-54-123-123-123.compute-1.amazonaws.com:9092
Don't forget to uncomment this line in the configuration file!
```

If you use only one broker with default configuration file **server.properties**, do this adjustment in it. If you use multiple brokers in the cluster with multiple configuration files, make this adjustment in each of the custom configuration files.

### Creating a new Maven project
```xml
 <groupId>com.stashchuk</groupId>
 <artifactId>Kafka</artifactId>
 <version>1.0-SNAPSHOT</version>

 <properties>
 	<maven.compiler.source>11</maven.compiler.source>
 	<maven.compiler.target>11</maven.compiler.target>
 	<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
 </properties>
```

### Starting Kafka Cluster
1. Start 3 brokers
- Clean Kafka intallation
- Copy config files from **\apache-kafka-course-master\EXAMPLES\02 Multiple Brokers**
- Run Zookeeper and Brokers
```
C:\kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server0.properties

C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server1.properties

C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server2.properties

```

### Creating Java Producer
- [Kafka Documentation](https://kafka.apache.org/34/)
- [Producer API](https://kafka.apache.org/34/apis/#producer-api)
- [Kafka Producer Config](https://kafka.apache.org/34/configuration/producer-configs/)
- [KafkaProducer](https://kafka.apache.org/34/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- Dependency:
```xml
	<dependency>
		<groupId>org.apache.kafka</groupId>
		<artifactId>kafka-clients</artifactId>
		<version>3.4.0</version>
	</dependency>

```
- Java implementation
```java
 Properties props = new Properties();
 props.put("bootstrap.servers", "localhost:9092");
 props.put("linger.ms", 1);
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
```
- Messages are stored in the brokers as sequences of bytes. That is why we need to specify Serializers on Producers and Deserializers on Consumers in order to correctly encode/decode message keys and values.

### Start a Producer
- Create a topic

```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092, localhost:9093, localhost:9094 --create --replication-factor 3 --partitions 5 --topic numbers

C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --list

C:\kafka> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic numbers

```
### Most common Producer parameters
- [Producer Configs](https://kafka.apache.org/34/configuration/producer-configs/)
- **key.serializer**, **value.serializer** - specify which serializer interface should be used for key and value encoding
- **acks** - if **acks=0** then th eporcucer sends messages without any acknowledgment form the server (don't recommended). **acks=1** This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. **acks=all** This means the leader will wait for the full set of in-sync replicas to acknowledge the record. 
- **bootstrap.servers** - A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. 
- **buffer.memory** - The total bytes of memory the producer can use to buffer records waiting to be sent to the server.
- **compression.type** - The compression type for all data generated by the producer. 
- **retries** - Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.
- **ssl.key.password** - The password of the private key in the key store file or the PEM key specified in 'ssl.keystore.key'.
- **ssl.keystore.key** - Private key in the format specified by 'ssl.keystore.type'.
- **ssl.keystore.location** - The location of the key store file. This is optional for client and can be used for two-way authentication for client.
- **ssl.keystore.password** - The store password for the key store file. This is optional for client and only needed if 'ssl.keystore.location' is configured.
- **ssl.truststore.certificates** - Trusted certificates in the format specified by 'ssl.truststore.type'. 
- **ssl.truststore.location** - The location of the trust store file.
- **ssl.truststore.password** - The password for the trust store file. 
- batch.size - is used to combine records into the fewer requests increasing throughput but may introduce additional latency. Batching is applied to the messages that are sent to the same partition.
- client.id - is used to add the custom name for the producer

### Modifying Serializer type

```
props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
```

### Producing meaningful messages with delay
- Create a new topic
```
C:\kafka> .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092, localhost:9093, localhost:9094 --create --replication-factor 3 --partitions 5 --topic strings
``` 

- Restart a consumer

```
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic strings
```

```java
  for (int i = 0; i < numOfRecords; i++) {
                String message = String.format("Producer %s has sent message %s at %s", clientId,i, new Date());
                System.out.println(message);
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), message));
                Thread.sleep(300);
            }
```