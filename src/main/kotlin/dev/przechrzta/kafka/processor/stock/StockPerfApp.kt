package dev.przechrzta.kafka.processor.stock

import dev.przechrzta.kafka.StreamsSerdes
import dev.przechrzta.kafka.mock.MockDataProducer
import dev.przechrzta.kafka.model.StockPerformance
import dev.przechrzta.kafka.processor.util.KStreamPrinter
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.Stores
import java.util.*

private val logger = KotlinLogging.logger {}
const val STOCK_TRX_TOPIC = "stock-transactions"
fun main() {
	val stringDes = Serdes.String().deserializer()
	val stringSer = Serdes.String().serializer()

	val stockPerfSerde = StreamsSerdes.stockPerformanceSerde()
	val stockPerfSerializer = stockPerfSerde.serializer()

	val stockTrxSerdes = StreamsSerdes.stockTransaction()
	val stockTrxDes = stockTrxSerdes.deserializer()

	val topology = Topology()
	val stockStateStore = "stock-performance-store"
	val differentialThreshold = 0.02
	val storeSupplier = Stores.inMemoryKeyValueStore(stockStateStore)
	val storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), stockPerfSerde)

	//debug
//	topology.addSource("stock-source", stringDes, stockTrxDes, STOCK_TRX_TOPIC)
//		.addProcessor("stocks-printer", KStreamPrinter<String, StockTransaction>("StockPerformance"), "stock-source")

	topology.addSource("stock-source", stringDes, stockTrxDes, STOCK_TRX_TOPIC)
		.addProcessor(
			"stocks-processor",
			ProcessorSupplier { StockPerformanceProcessor(stockStateStore, differentialThreshold) },
			"stock-source"
		)
		.addStateStore(storeBuilder, "stocks-processor")
		.addSink("stocks-sink", "stock-performance", stringSer, stockPerfSerializer, "stocks-processor");

	topology.addProcessor(
		"stocks-printer",
		KStreamPrinter<String, StockPerformance>("StockPerformance"),
		"stocks-processor"
	)

	logger.info { "Log analysis app started..." }
	val kafkaStreams = KafkaStreams(topology, stockProps())
	val generator = MockDataProducer()
	generator.produceStockTrx(50, 50, 25, { it.symbol })
	kafkaStreams.cleanUp()
	kafkaStreams.start()

	Thread.sleep(70000)
	logger.info { "Shutting down log analysis app" }
	kafkaStreams.close()
}

fun stockProps(): Properties {
	val props = Properties()
	props[ConsumerConfig.CLIENT_ID_CONFIG] = "stock--client"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "stock-group"
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "stock-app"

	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java

	return props
}
