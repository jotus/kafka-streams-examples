package dev.przechrzta.kafka.model

enum class Currency {
	EURO, DOLLAR, POUND
}
data class BeerPurchase(val currency: Currency, val totalSale: Double, val numberCases: Int, val beerType: String)
