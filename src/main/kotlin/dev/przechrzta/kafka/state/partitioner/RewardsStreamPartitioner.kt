package dev.przechrzta.kafka.state.partitioner

import dev.przechrzta.kafka.model.Purchase
import mu.KotlinLogging
import org.apache.kafka.streams.processor.StreamPartitioner

private val logger = KotlinLogging.logger {}
class RewardsStreamPartitioner : StreamPartitioner<String, Purchase> {
	override fun partition(topic: String, key: String?, value: Purchase, numPartitions: Int): Int {
		logger.info { "Partitionin for val: $value, numPart: $numPartitions" }
		return value.customerId.hashCode() % numPartitions
	}
}
