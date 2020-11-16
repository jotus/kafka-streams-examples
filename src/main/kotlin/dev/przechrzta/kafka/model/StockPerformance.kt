package dev.przechrzta.kafka.model

import java.text.DecimalFormat
import java.time.Instant
import java.util.*


class StockPerformance(
	var lastUpdateSent: Instant? = null,
	private var currentPrice: Double = 0.0
) {

	var shareDifferential: Double = 0.0
	    private set

	var priceDifferential: Double = 0.0
		private set
	var currentShareVolume = 0
		private set

	var currentAveragePrice = Double.MIN_VALUE
		private set
	var currentAverageVolume = Double.MIN_VALUE
		private set

	private val shareVolumeLookback =
		ArrayDeque<Double>(MAX_LOOK_BACK)

	private val sharePriceLookback =
		ArrayDeque<Double>(MAX_LOOK_BACK)


	@Transient
	private val decimalFormat = DecimalFormat("#.00")

	fun updatePriceStats(currentPrice: Double) {
		this.currentPrice = currentPrice
		priceDifferential = calculateDifferentialFromAverage(currentPrice, currentAveragePrice)
		currentAveragePrice = calculateNewAverage(currentPrice, currentAveragePrice, sharePriceLookback)
	}

	fun updateVolumeStats(currentShareVolume: Int) {
		this.currentShareVolume = currentShareVolume
		shareDifferential = calculateDifferentialFromAverage(currentShareVolume.toDouble(), currentAverageVolume)
		currentAverageVolume =
			calculateNewAverage(currentShareVolume.toDouble(), currentAverageVolume, shareVolumeLookback)
	}

	private fun calculateDifferentialFromAverage(value: Double, average: Double): Double {
		return if (average != Double.MIN_VALUE) (value / average - 1) * 100.0 else 0.0
	}

	private fun calculateNewAverage(
		newValue: Double,
		currentAverage: Double,
		deque: ArrayDeque<Double>
	): Double {
		var currentAverage = currentAverage
		if (deque.size < MAX_LOOK_BACK) {
			deque.add(newValue)
			if (deque.size == MAX_LOOK_BACK) {
				currentAverage = deque.stream().reduce(
					0.0
				) { a: Double, b: Double -> java.lang.Double.sum(a, b) } / MAX_LOOK_BACK
			}
		} else {
			val oldestValue = deque.poll()
			deque.add(newValue)
			currentAverage =
				currentAverage + newValue / MAX_LOOK_BACK - oldestValue / MAX_LOOK_BACK
		}
		return currentAverage
	}

	fun priceDifferential(): Double {
		return priceDifferential
	}



	override fun toString(): String {
		return "StockPerformance{" +
				"lastUpdateSent= " + lastUpdateSent +
				", currentPrice= " + decimalFormat.format(currentPrice) +
				", currentAveragePrice= " + decimalFormat.format(currentAveragePrice) +
				", percentage difference= " + decimalFormat.format(priceDifferential) +
				", currentShareVolume= " + decimalFormat.format(currentShareVolume.toLong()) +
				", currentAverageVolume= " + decimalFormat.format(currentAverageVolume) +
				", shareDifferential= " + decimalFormat.format(shareDifferential) +
				'}'
	}

	companion object {
		private const val MAX_LOOK_BACK = 20
	}
}
