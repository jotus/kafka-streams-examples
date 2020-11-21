package dev.przechrzta.kafka.model

import java.text.DecimalFormat
import java.util.concurrent.ThreadLocalRandom

data class PublicCompany(
	val lastSold: Double,
	val symbol: String,
	val name: String,
	val sector: String,
	val industry: String,
	var price: Double = 0.0
){
	val volatility = 0.1
	val formatter = DecimalFormat("#0.00")

	fun updateStockPrice(): Double {
		val min: Double = price * -volatility
		val max: Double = price * volatility
		val randomNum = ThreadLocalRandom.current().nextDouble(min, max + 1)
		price = price + randomNum
		return formatter.format(price).toDouble()
	}
}

