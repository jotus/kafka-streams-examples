package dev.przechrzta.kafka.state

import dev.przechrzta.kafka.model.Purchase
import dev.przechrzta.kafka.model.RewardAccumulator
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class RewardsTransformer(private val storeName: String) : ValueTransformer<Purchase, RewardAccumulator>{
	private lateinit var stateStore: KeyValueStore<String, Int>
	private lateinit var context: ProcessorContext

	override fun init(ctx: ProcessorContext) {
		this.context = ctx
		stateStore = this.context.getStateStore(storeName) as KeyValueStore<String, Int>
	}

	override fun transform(value: Purchase): RewardAccumulator {
		val accumulator = RewardAccumulator.fromPurchase(value)
		val accumulatedSoFar: Int? = stateStore.get(value.customerId)

		accumulatedSoFar?.let{
			accumulator.addRewardPoints(it)
		}
		stateStore.put(value.customerId, accumulator.totalRewardPoints)
		return accumulator
	}

	override fun close() {

	}

}
