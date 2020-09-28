package dev.przechrzta.kafka

import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Printed
import java.util.*


private val logger = KotlinLogging.logger {}

fun main() {

	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "count-application"
	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

	val config = StreamsConfig(props)
	val stringSerde = Serdes.String()
	val streamBuilder = StreamsBuilder()
	val upperCasedStream: KStream<String, String> = streamBuilder.stream<String, String>("simple-topic", Consumed.with(stringSerde, stringSerde))
		.mapValues{ message -> message.toUpperCase()}

	upperCasedStream.to("uppercased-topic")
	upperCasedStream.print(Printed.toSysOut<String, String>().withLabel("Yelling app"))

	val streams = KafkaStreams(streamBuilder.build(), config)
	logger.info { "Starting yelling app" }
	streams.start()

	Thread.sleep(350000)
	streams.close()



}
