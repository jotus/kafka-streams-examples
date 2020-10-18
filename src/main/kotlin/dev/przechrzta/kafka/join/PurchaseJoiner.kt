package dev.przechrzta.kafka.join

import dev.przechrzta.kafka.model.Purchase
import mu.KotlinLogging
import org.apache.kafka.streams.kstream.ValueJoiner
import java.util.*

private val logger = KotlinLogging.logger {}

class PurchaseJoiner : ValueJoiner<Purchase, Purchase, CorrelatedPurchase>{
	override fun apply(purchase: Purchase, otherPurchase: Purchase?): CorrelatedPurchase {
		val purchaseDate = purchase?.let { it.purchaseDate  }
		val price = purchase?.let { it.price * it.quantity }?: 0.0
		val itemPurchased = purchase?.let { it.itemPurchased }

		val otherPurchaseDate = otherPurchase?.let { it.purchaseDate  }
		val otherPrice = otherPurchase?.let { it.price * it.quantity }?: 0.0
		val otherItemPurchased = otherPurchase?.let { it.itemPurchased }
		val purchasedItems = mutableListOf<String>()
		itemPurchased?.also { purchasedItems.add(it) }
		otherItemPurchased?.also { purchasedItems.add(it) }
		val customerId = purchase?.customerId
		val otherCustomerId = otherPurchase?.customerId
		val result = CorrelatedPurchase.create(if(customerId != null) customerId else otherCustomerId,
			totalAmount = price + otherPrice,
			firstPurchaseTime = purchaseDate,
			secondPurchaseTime = otherPurchaseDate,
			itemsPurchased = purchasedItems)
		logger.info { "Joined item $result" }
		return result
	}

}
data class CorrelatedPurchase(private val customerId: String? ,
	private val itemsPurchased: List<String>,
	private val totalAmount: Double,
	private val firstPurchaseTime: Date? = null,
	private val secondPurchaseTime: Date? = null){
	companion object {
		fun create(customerId: String?, itemsPurchased: List<String> = emptyList(),
				   totalAmount: Double = 0.toDouble(),
				   firstPurchaseTime: Date? = null,
				   secondPurchaseTime: Date? = null
				   ) = CorrelatedPurchase(customerId, itemsPurchased, totalAmount, firstPurchaseTime, secondPurchaseTime)
	}
}
