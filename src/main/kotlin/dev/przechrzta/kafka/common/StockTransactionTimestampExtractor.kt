package dev.przechrzta.kafka.common

import dev.przechrzta.kafka.model.StockTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class StockTransactionTimestampExtractor : TimestampExtractor {
	override fun extract(record: ConsumerRecord<Any, Any>, previousTimestamp: Long): Long {
		return if( record.value() is StockTransaction){
			val transaction = record.value() as StockTransaction
			transaction.transactionTimestamp?.time ?: record.timestamp()
		} else{
			System.currentTimeMillis()
		}
	}
}
