package dev.przechrzta.kafka

import dev.przechrzta.kafka.infra.JsonDeserializer
import dev.przechrzta.kafka.infra.JsonSerializer
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import dev.przechrzta.kafka.model.Purchase
import dev.przechrzta.kafka.model.PurchasePattern
import dev.przechrzta.kafka.model.RewardAccumulator


object StreamsSerdes {
	fun purchaseSerde(): Serde<Purchase> = PurchaseSerde()
	fun purchasePatternSerde(): Serde<PurchasePattern> = PurchasePatternsSerde()
	fun rewardAccumulatorSerde(): Serde<RewardAccumulator> = RewardAccumulatorSerde()
}

class PurchaseSerde : WrapperSerde<Purchase>(JsonSerializer(), JsonDeserializer(Purchase::class.java))

class PurchasePatternsSerde :
	WrapperSerde<PurchasePattern>(JsonSerializer(), JsonDeserializer(PurchasePattern::class.java))

class RewardAccumulatorSerde :
	WrapperSerde<RewardAccumulator>(JsonSerializer(), JsonDeserializer(RewardAccumulator::class.java))


private class WrapperSerde<T> internal constructor(
	private val serializer: JsonSerializer<T>,
	private val deserializer: JsonDeserializer<T>
) : Serde<T> {

	override fun configure(map: Map<String, *>?, b: Boolean) {

	}

	override fun close() {

	}

	override fun serializer(): Serializer<T> {
		return serializer
	}

	override fun deserializer(): Deserializer<T> {
		return deserializer
	}
}
