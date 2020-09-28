package dev.przechrzta.kafka

import dev.przechrzta.kafka.model.Purchase
import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Printed
import java.util.*
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.Produced
import java.io.StreamCorruptedException


private val logger = KotlinLogging.logger {}

fun main() {
	val config = StreamsConfig(getProperties())
	val purchaseSerde = StreamsSerdes.purchaseSerde()
	val purchasePatternsSerde = StreamsSerdes.purchasePatternSerde()
	val rewardAccumulatorSerde = StreamsSerdes.rewardAccumulatorSerde()

	val builder = StreamsBuilder()
	val purchaseStream = builder.stream("transactions", Consumed.with(Serdes.String(), purchaseSerde))
		.mapValues { p -> p.maskCreditCard() }

	purchaseStream.print(Printed.toSysOut<String, Purchase>().withLabel("purchases"))
	purchaseStream.to("purchases", Produced.with(Serdes.String(), purchaseSerde))

	val kafkaStreams = KafkaStreams(builder.build(), config)
	kafkaStreams.start()

	Thread.sleep(65000)
	kafkaStreams.close()
}


fun getProperties(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "zmart-basic-app"
	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "zmart-purchases"
	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

	return props
}

