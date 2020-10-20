package dev.przechrzta.kafka

import dev.przechrzta.kafka.common.FixedSizePriorityQueue
import dev.przechrzta.kafka.infra.JsonDeserializer
import dev.przechrzta.kafka.infra.JsonSerializer
import dev.przechrzta.kafka.model.*
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer


object StreamsSerdes {
	fun purchaseSerde(): Serde<Purchase> = PurchaseSerde()
	fun purchasePatternSerde(): Serde<PurchasePattern> = PurchasePatternsSerde()
	fun rewardAccumulatorSerde(): Serde<RewardAccumulator> = RewardAccumulatorSerde()
	fun stockTicker(): Serde<StockTickerData> = StockTickerSerde()
	fun stockTransaction(): Serde<StockTransaction> = StockTransactionSerde()
	fun shareVolume(): Serde<ShareVolume> = ShareVolumeSerde()
	fun priorityQueue(): Serde<FixedSizePriorityQueue> = FixedSizePriorityQueueSerde()
}

class FixedSizePriorityQueueSerde : WrapperSerde<FixedSizePriorityQueue>(JsonSerializer(), JsonDeserializer(FixedSizePriorityQueue::class.java))

class ShareVolumeSerde : WrapperSerde<ShareVolume>(JsonSerializer(), JsonDeserializer(ShareVolume::class.java))


class StockTransactionSerde: WrapperSerde<StockTransaction>(JsonSerializer(), JsonDeserializer(StockTransaction::class.java))

class StockTickerSerde : WrapperSerde<StockTickerData>(JsonSerializer(), JsonDeserializer(StockTickerData::class.java))

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
