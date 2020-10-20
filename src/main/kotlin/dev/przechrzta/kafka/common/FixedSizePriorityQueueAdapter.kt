package dev.przechrzta.kafka.common


import com.google.gson.Gson
import com.google.gson.TypeAdapter
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import dev.przechrzta.kafka.model.ShareVolume
import java.io.IOException
import java.util.*


class FixedSizePriorityQueueAdapter : TypeAdapter<FixedSizePriorityQueue>() {
	private val gson = Gson()

	@Throws(IOException::class)
	override fun write(writer: JsonWriter, value: FixedSizePriorityQueue) {
		if (value == null) {
			writer.nullValue()
			return
		}
		val iterator: Iterator<ShareVolume?> = value.iterator()
		val list: MutableList<ShareVolume?> = mutableListOf()
		while (iterator.hasNext()) {
			val stockTransaction: ShareVolume? = iterator.next()
			if (stockTransaction != null) {
				list.add(stockTransaction)
			}
		}
		writer.beginArray()
		for (transaction in list) {
			writer.value(gson.toJson(transaction))
		}
		writer.endArray()
	}

	@Throws(IOException::class)
	override fun read(reader: JsonReader): FixedSizePriorityQueue? {
		val list: MutableList<ShareVolume> = mutableListOf()
		reader.beginArray()
		while (reader.hasNext()) {
			list.add(gson.fromJson(reader.nextString(), ShareVolume::class.java))
		}
		reader.endArray()
		val c: Comparator<ShareVolume> = Comparator { c1: ShareVolume, c2: ShareVolume -> c2.shares - c1.shares }
		val fixedSizePriorityQueue = FixedSizePriorityQueue(5,c)
		for (transaction in list) {
			fixedSizePriorityQueue.add(transaction)
		}
		return fixedSizePriorityQueue
	}
}
