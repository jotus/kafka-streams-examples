package dev.przechrzta.kafka.infra

import com.google.gson.GsonBuilder
import com.google.gson.Gson
import org.apache.kafka.common.serialization.Deserializer
import java.lang.reflect.Type

class JsonDeserializer<T> : Deserializer<T> {
	private var gson: Gson? = null
	private var deserializedClass: Class<T>? = null
	private var reflectionTypeToken: Type? = null

	constructor(deserializedClass: Class<T>) {
		this.deserializedClass = deserializedClass
		init()
	}

	constructor(reflectionTypeToken: Type) {
		this.reflectionTypeToken = reflectionTypeToken
		init()
	}

	private fun init() {
		val builder = GsonBuilder()
//		builder.registerTypeAdapter(FixedSizePriorityQueue::class.java, FixedSizePriorityQueueAdapter().nullSafe())
		gson = builder.create()
	}

	override fun configure(map: Map<String, *>?, b: Boolean) {
		if (deserializedClass == null) {
			deserializedClass = map!!["serializedClass"] as Class<T>
		}
	}

	override fun deserialize(s: String, bytes: ByteArray?): T? {
		if (bytes == null) {
			return null
		}

		val deserializeFrom = if (deserializedClass != null) deserializedClass else reflectionTypeToken
		return gson!!.fromJson(String(bytes), deserializeFrom)
	}

	override fun close() {
	}
}
