```cmd
C:\Kafka\kafka_2.13-2.7.0

# Start Zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Start Kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

# Create topic TestTopic
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic TestTopic

# List all topics
.\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181

# Start producer
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic TestTopic

# Start consumer
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic TestTopic --from-beginning
```
