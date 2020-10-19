./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic CountsTopic  --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer

./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transactions  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic CountsTopic  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

./kafka-topics.sh --zookeeper localhost:2181 --alter --topic transactions --config retention.ms=1000
#./kafka-configs.sh

./kafka-topics.sh --zookeeper localhost:2181 --list


kafkacat -P -b localhost -t simple-topic

./kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092
./kafka-topics.sh --describe --topic transactions --bootstrap-server localhost:9092

# Creating topic
./kafka-topics.sh --create --topic transactions --zookeeper localhost:2181 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic patterns --zookeeper localhost:2181 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic rewards --zookeeper localhost:2181 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic customer_trx --zookeeper localhost:2181 --partitions 1 --replication-factor 1


./kafka-topics.sh --create --topic coffee --zookeeper localhost:2181 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic electronics --zookeeper localhost:2181 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic expensive_purchases --zookeeper localhost:2181 --partitions 1 --replication-factor 1


./kafka-topics.sh --describe --topic transactions --zookeeper localhost:2181


./kafka-topics.sh --create --topic purchases --zookeeper localhost:2181 --partitions 1 --replication-factor 1

kafkacat -b localhost:9092 -t <my_topic> -T -P -l /tmp/msgs
kafkacat -b localhost:9092 -t keyed_topic -P -K:

## producing data to topic
kafkacat -b localhost:9092 -t transactions -T -P -l /Users/wojciech.przechrzta/myDevel/kafka_workspace/kafka-examples-master/data.json
kafkacat -b localhost:9092 -t transactions -T -P -l /Users/wojciech.przechrzta/myDevel/kafka_workspace/kafka-examples-master/src/main/resources/purchasesWithDate.json
## consuming
kafkacat -C -b localhost:9092 -t purchases
kafkacat -b localhost:9092 -t mysql_users -C -c1 -K:
1   {"uid":1,"name":"Cliff","locale":"en_US","address_city":"St Louis","elite":"P"}
