package dev.przechrzta.kafka

import dev.przechrzta.kafka.model.Purchase
import dev.przechrzta.kafka.model.PurchasePattern
import dev.przechrzta.kafka.model.RewardAccumulator
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import java.util.*


private val logger = KotlinLogging.logger {}


fun main() {
	val config = StreamsConfig(getPropertiesMulti())
	val purchaseSerde = StreamsSerdes.purchaseSerde()
	val purchasePatternsSerde = StreamsSerdes.purchasePatternSerde()
	val rewardAccumulatorSerde = StreamsSerdes.rewardAccumulatorSerde()

	val builder = StreamsBuilder()
	val trxStream = builder.stream("transactions", Consumed.with(Serdes.String(), purchaseSerde))


	val maskedCreditCard = trxStream.mapValues { p -> p.maskCreditCard() }

	maskedCreditCard.print(Printed.toSysOut<String, Purchase>().withLabel("purchases"))
	maskedCreditCard.to("purchases", Produced.with(Serdes.String(), purchaseSerde))

	val purchasePatternStream: KStream<String, PurchasePattern> =
		maskedCreditCard.mapValues { p -> PurchasePattern.fromPurchase(p) }

	purchasePatternStream.print(Printed.toSysOut<String, PurchasePattern>().withLabel("patterns"))
	purchasePatternStream.to("patterns", Produced.with(Serdes.String(), purchasePatternsSerde))

	val rewardsStream: KStream<String, RewardAccumulator> =
		maskedCreditCard.mapValues { p -> RewardAccumulator.fromPurchase(p) }

	rewardsStream.print(Printed.toSysOut<String, RewardAccumulator>().withLabel("rewards"))
	rewardsStream.to("rewards", Produced.with(Serdes.String(), rewardAccumulatorSerde))

	//selecting key
	val purchaseDayAsKeyMappper = KeyValueMapper { key: String?, purchase: Purchase -> purchase.purchaseDate.time }
	val filteredKstream: KStream<Long, Purchase> =
		maskedCreditCard.filter { key: String?, purchase -> purchase.price > 5.00 }
			.selectKey(purchaseDayAsKeyMappper)

//	//==========FILTERING
	filteredKstream.print(Printed.toSysOut<Long, Purchase>().withLabel("expensive_purchases"))
	filteredKstream.to("expensive_purchases", Produced.with(Serdes.Long(), purchaseSerde))

	//==========BRANCHING
	val isCofee = Predicate { key: String?, value: Purchase -> value.department.equals("coffee", true) }
	val isElectronics = Predicate { key: String?, value: Purchase -> value.department.equals("electronics", true) }
	val coffee = 0
	val electronics = 1
	val kstreamByDept: Array<KStream<String?, Purchase>> = maskedCreditCard.branch(isCofee, isElectronics)

	kstreamByDept[coffee].print(Printed.toSysOut<String?, Purchase>().withLabel("coffee"))
	kstreamByDept[coffee].to("coffee", Produced.with(Serdes.String(), purchaseSerde))

	kstreamByDept[electronics].print(Printed.toSysOut<String?, Purchase>().withLabel("electronics"))
	kstreamByDept[electronics].to("electronics", Produced.with(Serdes.String(), purchaseSerde))


	val kafkaStreams = KafkaStreams(builder.build(), config)
	kafkaStreams.start()

	Thread.sleep(650000)
	kafkaStreams.close()
}


fun getPropertiesMulti(): Properties {
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

