package dev.przechrzta.kafka.model

import java.lang.StringBuilder

data class RewardAccumulator (
	val customerId: String,
	val purchaseTotal: Double,
	val rewardsPoints: Int
) {

	var currentRewardPoints: Int = rewardsPoints
	var totalRewardPoints: Int = rewardsPoints
	private var daysFromLastPurchase: Int = 0


	fun addRewardPoints(previousTotalPoints: Int) {
		this.totalRewardPoints += previousTotalPoints
	}

	companion object {
		fun fromPurchase(p: Purchase): RewardAccumulator{
			val total = p.price * p.quantity
			return RewardAccumulator(StringBuilder(p.lastName).append(",").append(p.firstName).toString(), total, total.toInt() )
		}
	}


}
