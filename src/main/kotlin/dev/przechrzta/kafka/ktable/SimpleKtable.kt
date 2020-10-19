package dev.przechrzta.kafka.ktable

import dev.przechrzta.kafka.StreamsSerdes
import dev.przechrzta.kafka.model.StockTickerData
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import java.util.*

const val STOCK_TICKER_TABLE_TOPIC = "stock-ticker-table"
const val STOCK_TICKER_STREAM_TOPIC = "stock-ticker-stream"

private val logger = KotlinLogging.logger {}

fun main(){

	val streamsConfig = StreamsConfig(simpleKtableProps())
	val builder = StreamsBuilder()

	val stockTickerTable: KTable<String, StockTickerData> = builder.table(STOCK_TICKER_TABLE_TOPIC)
	val stockTickerStream: KStream<String, StockTickerData> = builder.stream(STOCK_TICKER_STREAM_TOPIC)

	stockTickerTable.toStream().print(Printed.toSysOut<String,StockTickerData>().withLabel("StocksTable"))
	stockTickerStream.print(Printed.toSysOut<String,StockTickerData>().withLabel("StocksStream"))

	val kafkastreams = KafkaStreams(builder.build(), streamsConfig)
	kafkastreams.cleanUp()
	kafkastreams.start()

	logger.info { "KStream and Ktable app started" }

	Thread.sleep(65000)
	logger.info { "Shutting dow KStreams and KTable now..." }
	kafkastreams.close()

}

fun simpleKtableProps(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "simple-ktable-app"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "simple-ktable-group"
	props[ConsumerConfig.CLIENT_ID_CONFIG] = "simple-ktable-client"
	props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
	props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = "30000"
	props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "15000"

	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = "1"
	props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = "10000"
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = StreamsSerdes.stockTicker().javaClass

	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java

	return props
}
