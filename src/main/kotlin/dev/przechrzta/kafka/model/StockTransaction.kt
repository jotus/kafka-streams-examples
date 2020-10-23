package dev.przechrzta.kafka.model

import java.util.*

data class StockTransaction(
	val symbol: String,
	val sector: String,
	val industry: String,
	val customerId: String,
	val shares: Int = 0,
	val sharePrice: Double = 0.0,
	val transactionTimestamp: Date? = null,
	val purchase: Boolean = false
) {

	companion object {
		fun reduce(left: StockTransaction, right: StockTransaction) = left.copy(shares = left.shares + right.shares)
	}
}
