package dev.przechrzta.kafka.infra

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.Charset

class JsonSerializer<T> : Serializer<T> {
	private val gson: Gson

	init {
		val builder = GsonBuilder()
//		builder.registerTypeAdapter(FixedSizePriorityQueue::class.java, FixedSizePriorityQueueAdapter().nullSafe())
		gson = builder.create()
	}

	override fun configure(map: Map<String, *>, b: Boolean) {
	}

	override fun serialize(topic: String, t: T): ByteArray {
		return gson.toJson(t).toByteArray(Charset.forName("UTF-8"))
	}

	override fun close() {
	}
}
