package dev.przechrzta.kafka.processor

import org.apache.kafka.streams.processor.AbstractProcessor

class MapNameProcessor<K, V, VR>(val mapper: Function1<V, VR>) : AbstractProcessor<K, V>() {
	override fun process(key: K, value: V) {
		this.context().forward(key, mapper.invoke(value))
	}
}
