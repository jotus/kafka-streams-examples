package dev.przechrzta.kafka.window

import dev.przechrzta.kafka.StreamsSerdes
import dev.przechrzta.kafka.common.StockTransactionTimestampExtractor
import dev.przechrzta.kafka.model.StockTransaction
import dev.przechrzta.kafka.model.TransactionSummary
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
import java.time.Duration
import java.util.*


const val STOCK_TRANSACTIONS_TOPIC = "stock-transactions"
const val FINANCIAL_NEWS = "financial-news"

fun main() {
	val streamsConfig = StreamsConfig(windowedProps())

	val stringSerde = Serdes.String()
	val transactionSerde = StreamsSerdes.stockTransaction()
	val transactionKeySerde = StreamsSerdes.transactionSummary()

	val builder = StreamsBuilder()
	val twentySec: Long = 20*1000
	val fifteenMinutes: Long = 15 *60 * 1000
	val fiveSeconds = 5 * 1000

	val  customerTransactionCounts: KTable<Windowed<TransactionSummary>, Long> =
		builder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde,transactionSerde)
		.withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
		.groupBy({ noKey: String?, value: StockTransaction ->
			TransactionSummary.fromTransaction(value)
		}, Grouped.with(transactionKeySerde, transactionSerde))
//		.windowedBy(SessionWindows.with(Duration.ofSeconds(twentySec)))
		.windowedBy(TimeWindows.of(Duration.ofSeconds(twentySec)))//Tumbling window
		.count()

	customerTransactionCounts.toStream()
		.print(Printed.toSysOut<Windowed<TransactionSummary>, Long>().withLabel("trx-summary"))

	//Repartition
	val countStream: KStream<String, TransactionSummary> =customerTransactionCounts.toStream().map{ window: Windowed<TransactionSummary>, value: Long ->
		val trxSummary = window.key()
		val newKey = trxSummary.industry
		trxSummary.summaryCount = value
		KeyValue.pair(newKey, trxSummary)
	}

	val financialNews: KTable<String, String> = builder.table(FINANCIAL_NEWS, Consumed.with(Topology.AutoOffsetReset.EARLIEST))
	val valueJoiner = ValueJoiner{ txnct: TransactionSummary, news: String? ->
			"${txnct.summaryCount} shares purchased ${txnct.stockTicker} related news [${news}]"
	}

	val joinedWithNews: KStream<String, String> = countStream.leftJoin(financialNews, valueJoiner, Joined.with(stringSerde, transactionKeySerde, stringSerde))

	joinedWithNews.print(Printed.toSysOut<String, String>().withLabel("with-news"))

	val kafkaStreams = KafkaStreams(builder.build(), streamsConfig)
	kafkaStreams.cleanUp()
	kafkaStreams.start()

	Thread.sleep(65000)
	kafkaStreams.close()
}

fun windowedProps(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "windowed-aggregation-app"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "windowed-aggregation-group"
	props[ConsumerConfig.CLIENT_ID_CONFIG] = "windowed-aggregation-client"

	props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
	props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = "30000"
	props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "10000"

	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = "1"
	props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = "10000"
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = StockTransactionTimestampExtractor::class.java

	return props
}
