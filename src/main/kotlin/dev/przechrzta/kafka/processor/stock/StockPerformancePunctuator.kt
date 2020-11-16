package dev.przechrzta.kafka.processor.stock

import dev.przechrzta.kafka.model.StockPerformance
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.Punctuator
import org.apache.kafka.streams.state.KeyValueStore

class StockPerformancePunctuator(
	val threshold: Double,
	val context: ProcessorContext,
	val keyValueStore: KeyValueStore<String, StockPerformance>) : Punctuator {

	override fun punctuate(timestamp: Long) {
		val iterator = keyValueStore.all()

		while (iterator.hasNext()) {
			val keyValue = iterator.next()
			val stockPerformance = keyValue.value
			val key = keyValue.key

			stockPerformance?.let {
				if(it.priceDifferential >= threshold ||
						it.shareDifferential >= threshold){
					context.forward(key, keyValue)
				}
			}
		}
	}
}
