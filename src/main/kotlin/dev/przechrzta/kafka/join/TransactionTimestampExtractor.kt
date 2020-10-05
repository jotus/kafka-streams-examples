package dev.przechrzta.kafka.join

import dev.przechrzta.kafka.model.Purchase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class TransactionTimestampExtractor: TimestampExtractor{
	override fun extract(record: ConsumerRecord<Any, Any>, previousTimestamp: Long): Long {
		val purchase  = record.value() as Purchase
		return purchase.purchaseDate.time
	}

}
