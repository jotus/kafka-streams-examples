package dev.przechrzta.kafka.processor.util

import mu.KotlinLogging
import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorSupplier

private val logger = KotlinLogging.logger {}

class KStreamPrinter<K, V>(val name: String) : ProcessorSupplier<K,V> {
	override fun get(): Processor<K, V> {
		return PrintingProcessor(name)
	}

	class PrintingProcessor<K,Val>(val name: String): AbstractProcessor<K,Val>(){
		override fun process(key: K, value: Val) {
			logger.info { "--[$name] Key: [$key], Value: [$value]" }
			this.context().forward(key, value)
		}
	}
}
