package dev.przechrzta.kafka.state

import dev.przechrzta.kafka.StreamsSerdes
import dev.przechrzta.kafka.model.Purchase
import dev.przechrzta.kafka.model.RewardAccumulator
import dev.przechrzta.kafka.state.partitioner.RewardsStreamPartitioner
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueTransformerSupplier
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import java.util.*

fun main() {
	val config = StreamsConfig(getProperties())
	val purchaseSerde = StreamsSerdes.purchaseSerde()
	val stringSerde = Serdes.String()
	val intSerde = Serdes.Integer()
	val rewardAccumulatorSerde = StreamsSerdes.rewardAccumulatorSerde()

	val builder = StreamsBuilder()
	val trxStream = builder.stream("transactions", Consumed.with(Serdes.String(), purchaseSerde))
		.mapValues { p -> p.maskCreditCard() }

	trxStream.print(Printed.toSysOut<String, Purchase>().withLabel("purchases"))
	trxStream.to("purchases", Produced.with(Serdes.String(), purchaseSerde))

	val storeName = "rewardsPointsStore"
	val storeSupplier = Stores.inMemoryKeyValueStore(storeName)
	val storeBuilder: StoreBuilder<KeyValueStore<String, Int>> =
		Stores.keyValueStoreBuilder(storeSupplier, stringSerde, intSerde)
	builder.addStateStore(storeBuilder)

	// Repartitioning happens here
	val streamPartitioner = RewardsStreamPartitioner()
	val transByCustomer =
		trxStream.through("customer_trx", Produced.with(stringSerde, purchaseSerde, streamPartitioner))

	val statefulRewardsAccumulator = transByCustomer.transformValues(ValueTransformerSupplier { RewardsTransformer(storeName) }, storeName)
	statefulRewardsAccumulator.print(Printed.toSysOut<String, RewardAccumulator>().withLabel("rewards"))
	statefulRewardsAccumulator.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde))

	val kafkaStreams = KafkaStreams(builder.build(), config)
	kafkaStreams.start()
	Thread.sleep(650000)
	kafkaStreams.close()
}

fun getProperties(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "zmart-multiple-app"
	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "zmart-multi-purchases"
	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

	return props
}
