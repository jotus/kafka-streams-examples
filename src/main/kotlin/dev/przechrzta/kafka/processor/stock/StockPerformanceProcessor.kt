package dev.przechrzta.kafka.processor.stock

import dev.przechrzta.kafka.model.StockPerformance
import dev.przechrzta.kafka.model.StockTransaction
import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Duration
import java.time.Instant


class StockPerformanceProcessor(val stateStoreName: String, val threshold: Double) : AbstractProcessor<String, StockTransaction>() {
	private  lateinit var keyValueStore: KeyValueStore<String, StockPerformance>

	override fun init(context: ProcessorContext) {
		super.init(context)
		keyValueStore = context().getStateStore(stateStoreName) as KeyValueStore<String, StockPerformance>

		val punctuator = StockPerformancePunctuator(threshold, context(), keyValueStore )
		context().schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, punctuator)
	}

	override fun process(symbol: String?, transaction: StockTransaction) {
		symbol?.let {
			val stockPerformance = keyValueStore.get(symbol)?: StockPerformance()

			stockPerformance.updatePriceStats(transaction.sharePrice)
			stockPerformance.updateVolumeStats(transaction.shares)
			stockPerformance.lastUpdateSent = Instant.now()

			keyValueStore.put(symbol, stockPerformance)
		}
	}
}
