package dev.przechrzta.kafka.model

data class PublicCompany(
	val lastSold: Double,
	val symbol: String,
	val name: String,
	val sector: String,
	val industry: String,
	val price: Double = 0.0
)

