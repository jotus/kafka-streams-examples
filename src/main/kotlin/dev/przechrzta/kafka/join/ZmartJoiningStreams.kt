package dev.przechrzta.kafka.join

import dev.przechrzta.kafka.StreamsSerdes
import dev.przechrzta.kafka.model.Purchase
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*


private val logger = KotlinLogging.logger {}


fun main() {
	val config = StreamsConfig(getPropertiesJoin())
	val purchaseSerde = StreamsSerdes.purchaseSerde()
	val stringSerde = Serdes.String()

	val keyValueMapper: KeyValueMapper<String, Purchase, KeyValue<String, Purchase>> =
		KeyValueMapper { k, v: Purchase ->
			val masked = v.maskCreditCard()
			KeyValue(masked.customerId, masked)
		}

	val builder = StreamsBuilder()
	val trxStream = builder.stream("transactions", Consumed.with(Serdes.String(), purchaseSerde))
		.map(keyValueMapper)


	val isCofee = Predicate { key: String?, value: Purchase -> value.department.equals("coffee", true) }
	val isElectronics = Predicate { key: String?, value: Purchase -> value.department.equals("electronics", true) }
	val coffee = 0
	val electronics = 1
	val branchedStream: Array<KStream<String?, Purchase>> = trxStream.selectKey({ key: String, value: Purchase ->
		value.customerId
	}).branch(isCofee, isElectronics)

	branchedStream[coffee].print(Printed.toSysOut<String?, Purchase>().withLabel("coffee"))
	branchedStream[electronics].print(Printed.toSysOut<String?,Purchase>().withLabel("electro"))
	val coffeeStream: KStream<String?, Purchase> = branchedStream[coffee]
	val electronicsStream: KStream<String?, Purchase> = branchedStream[electronics]

	val purchaseJoiner = PurchaseJoiner()
	val twentyMinutesWindow = JoinWindows.of(60 * 1000 * 20)

	val joinedKStream = coffeeStream.join(
		electronicsStream,
		purchaseJoiner,
		twentyMinutesWindow,
		Joined.with(stringSerde, purchaseSerde, purchaseSerde)
	)

	joinedKStream.print(Printed.toSysOut<String, CorrelatedPurchase>().withLabel("correlated"))

	logger.info { "Starting join example" }
	val kafkaStreams = KafkaStreams(builder.build(), config)
	kafkaStreams.start()
	Thread.sleep(65000)
	logger.info { "Shotting down join example" }
	kafkaStreams.close()
}

fun getPropertiesJoin(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "zmart-join-app"
	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = "1"
	props[StreamsConfig.METADATA_MAX_AGE_CONFIG] = "10000"
	props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
	props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "zmart-join-purchases"
	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = TransactionTimestampExtractor::class.java
	return props
}
