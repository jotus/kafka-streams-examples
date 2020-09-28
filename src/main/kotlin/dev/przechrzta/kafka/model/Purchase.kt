package dev.przechrzta.kafka.model

import java.util.*


private const val CC_NUMBER_REPLACEMENT = "xxxx-xxxx-xxxx-"

data class Purchase(
	val firstName: String,
	val lastName: String,
	val customerId: String,
	val creditCardNumber: String,
	val itemPurchased: String,
	val department: String,
	val employeeId: String,
	val quantity: Int,
	val price: Double,
	val zipCode: String,
	val storeId: String
) {

	fun maskCreditCard(): Purchase {
		Objects.requireNonNull<Any>(creditCardNumber, "Credit Card can't be null")
		val parts = this.creditCardNumber.split("-")
		if (parts.size < 4) {
			return this.copy(creditCardNumber = "xxxx")
		} else {
			val last4Digits = this.creditCardNumber.split("-")[3]
			return this.copy(creditCardNumber = CC_NUMBER_REPLACEMENT + last4Digits)
		}
	}
}
