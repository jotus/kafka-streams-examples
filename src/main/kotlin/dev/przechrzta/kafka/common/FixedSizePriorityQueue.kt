package dev.przechrzta.kafka.common

import dev.przechrzta.kafka.model.ShareVolume
import java.util.*

class FixedSizePriorityQueue(val maxSize: Int, comparator: Comparator<ShareVolume>) {
	val inner: TreeSet<ShareVolume>

	init {
		inner = TreeSet(comparator)
	}

	fun add(e: ShareVolume): FixedSizePriorityQueue {
		this.inner.add(e)
		if (inner.size > maxSize) {
			this.inner.pollLast()
		}
		return this
	}

	fun remove(e: ShareVolume): FixedSizePriorityQueue {
		if (this.inner.contains(e)) {
			this.inner.remove(e)
		}
		return this
	}

	fun iterator() = this.inner.iterator()

}
