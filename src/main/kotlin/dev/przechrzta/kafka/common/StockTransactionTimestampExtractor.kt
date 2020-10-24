package dev.przechrzta.kafka.common

import dev.przechrzta.kafka.model.StockTransaction
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

private val logger = KotlinLogging.logger {}

class StockTransactionTimestampExtractor : TimestampExtractor {
	override fun extract(record: ConsumerRecord<Any, Any>, previousTimestamp: Long): Long {
		return if( record.value() is StockTransaction){
			val transaction = record.value() as StockTransaction
			logger.info { "Stock: ${transaction}, timstamp:${transaction.transactionTimestamp?.time}" }
			transaction.transactionTimestamp?.time ?: record.timestamp()
		} else{
			System.currentTimeMillis()
		}
	}
}
