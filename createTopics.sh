
echo "Creating topics ..."

KAFKA_HOME=/Users/jotus/myInstall/kafka
KAFKA_HOME=/Users/wojciech.przechrzta/myInstall/kafka_2.12-1.0.0
KAFKA_BIN=$KAFKA_HOME/bin
#./kafka-topics.sh --create --topic transactions --zookeeper localhost:2181 --partitions 1 --replication-factor 1
#./kafka-topics.sh --create --topic patterns --zookeeper localhost:2181 --partitions 1 --replication-factor 1
#./kafka-topics.sh --create --topic rewards --zookeeper localhost:2181 --partitions 1 --replication-factor 1
#./kafka-topics.sh --create --topic customer_trx --zookeeper localhost:2181 --partitions 1 --replication-factor 1

function create_topic() {
	if exists $1 ;then
		echo "topic $1 already created, skipping..."
		return 0
  fi

    echo "Creating topic $1"

    ${KAFKA_BIN}/kafka-topics.sh --create --topic $1 --zookeeper localhost:2181 --partitions 1 --replication-factor 1

    echo "Created topic $1"
}

function exists(){
	 test "$($KAFKA_BIN/kafka-topics.sh --zookeeper localhost:2181 --list | grep $1)" == "$1";
}

function list_topics() {
    $KAFKA_BIN/kafka-topics.sh --zookeeper localhost:2181 --list
}

#create_topic "transactions"
#create_topic "patterns"
#create_topic "rewards"
#create_topic "purchases"
#create_topic "customer_trx"
#create_topic "stock-ticker-stream"
#create_topic "stock-ticker-table"
create_topic "stock-transactions"
create_topic ""stock-performance""
#create_topic "stock-volume-by-company"
#create_topic "financial-news"
#create_topic "family-names"
#create_topic "pops-hops-purchases"

list_topics
