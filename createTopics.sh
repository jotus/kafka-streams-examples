
echo "Creating topics ..."

KAFKA_HOME=/Users/jotus/myInstall/kafka
KAFKA_BIN=$KAFKA_HOME/bin
#./kafka-topics.sh --create --topic transactions --zookeeper localhost:2181 --partitions 1 --replication-factor 1
#./kafka-topics.sh --create --topic patterns --zookeeper localhost:2181 --partitions 1 --replication-factor 1
#./kafka-topics.sh --create --topic rewards --zookeeper localhost:2181 --partitions 1 --replication-factor 1
#./kafka-topics.sh --create --topic customer_trx --zookeeper localhost:2181 --partitions 1 --replication-factor 1

function create_topic() {
    echo "Creating topic $1"
    $($KAFKA_BIN/kafka-topics.sh --create --topic transactions --zookeeper localhost:2181 --partitions 1 --replication-factor 1)
    echo "Created topic $1"
}

create_topic "transactions"
create_topic "patterns"
create_topic "rewards"
create_topic "customer_trx"
