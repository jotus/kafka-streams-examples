package dev.przechrzta.kafka.processor


import dev.przechrzta.kafka.infra.JsonDeserializer
import dev.przechrzta.kafka.model.BeerPurchase
import dev.przechrzta.kafka.processor.util.KStreamPrinter
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.processor.UsePreviousTimeOnInvalidTimestamp
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import java.util.*

const val domesticSalesSink = "domestic-beer-sales"
const val internationalSalesSink = "international-beer-sales"

const val purchaseSourceNodeName = "beer-purchase-source"
const val purchaseProcessor = "purchase-processor"

const val sourceTopic = "pops-hops-purchases"

fun main() {
	val stringDes= Serdes.String().deserializer()
	val beerDes = JsonDeserializer(BeerPurchase::class.java)
	val beerProcessor = BeerProcessor(domesticSalesSink, internationalSalesSink)

	val topology = Topology()

	topology.addSource(Topology.AutoOffsetReset.LATEST,
		purchaseSourceNodeName,
		UsePreviousTimeOnInvalidTimestamp(),
		stringDes,
		beerDes,
		sourceTopic
	).addProcessor(purchaseProcessor,
		ProcessorSupplier { beerProcessor },
		purchaseSourceNodeName)

	topology.addProcessor(domesticSalesSink,
		KStreamPrinter<String, BeerPurchase>("beer-domestic"),
		purchaseProcessor
		)
	topology.addProcessor(internationalSalesSink,
		KStreamPrinter<String, BeerPurchase>("beer-intern"),
		purchaseProcessor
	)

	val kafkaStreams= KafkaStreams(topology, beerProps())
	kafkaStreams.start()

	Thread.sleep(65000)

	kafkaStreams.close()
}

fun beerProps(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "beer-aggregation-app"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "beer-aggregation-group"
	props[ConsumerConfig.CLIENT_ID_CONFIG] = "beer-aggregation-client"

	props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
	props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = "30000"
	props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "10000"

	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = "1"
	props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = "10000"
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java

	return props
}
