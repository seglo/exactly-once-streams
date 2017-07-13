#### List topics

```
kafka-topics.sh --zookeeper zookeeper:2181 --list
```

#### Create a topic
NOTE: `replication-factor` cannot be > number of brokers
```
kafka-topics.sh --zookeeper zookeeper:2181 --create --topic datasource --partitions 3 --replication-factor 3
```
#### Console consumer

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic datasource
```
