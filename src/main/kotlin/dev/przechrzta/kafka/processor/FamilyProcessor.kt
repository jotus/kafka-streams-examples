package dev.przechrzta.kafka.processor

import dev.przechrzta.kafka.processor.util.KStreamPrinter
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.internals.KStreamPrint
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import java.util.*


const val FAMILY_NAMES = "family-names"
fun main() {


	val stringSer = Serdes.String().serializer()
	val stringDes = Serdes.String().deserializer()

	val nameMapper = { name: String -> "$name (${name.length})"}
	val processorSupplier = ProcessorSupplier {MapNameProcessor<String, String, String>(nameMapper)}
	val topology = Topology()
	topology.addSource("family", FAMILY_NAMES)

	topology.addProcessor("name-to-age", processorSupplier, "family")
	topology.addProcessor("print-sink", KStreamPrinter<String, String>("print-fam"), "name-to-age")

	val kafkaStreams = KafkaStreams(topology, familyProps())
	kafkaStreams.start()

	Thread.sleep(60000)
	kafkaStreams.close()
}

fun familyProps(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "fam-aggregation-app"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "fam-aggregation-group"
	props[ConsumerConfig.CLIENT_ID_CONFIG] = "fam-aggregation-client"

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
