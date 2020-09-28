package dev.przechrzta.kafka.model

import java.util.*

data class PurchasePattern(
	private val zipCode: String,
	private val item: String,
	private val date: Date,
	private val amount: Double
)
