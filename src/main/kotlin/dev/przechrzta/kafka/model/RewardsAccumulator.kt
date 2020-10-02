package dev.przechrzta.kafka.model

import java.lang.StringBuilder

data class RewardAccumulator (
	val customerId: String,
	val purchaseTotal: Double,
	val rewardsPoints: Int
) {

	var totalRewardPoints: Int = rewardsPoints
	private var daysFromLastPurchase: Int = 0


	fun addRewardPoints(previousTotalPoints: Int): RewardAccumulator {
		this.totalRewardPoints += previousTotalPoints
		return this
	}

	companion object {
		fun fromPurchase(p: Purchase): RewardAccumulator{
			val total = p.price * p.quantity
			return RewardAccumulator(p.customerId, total, total.toInt() )
		}
	}

	override fun toString(): String {
		return "RewardAccumulator(customerId=$customerId, purchaseTotal=$purchaseTotal, rewardsPoints=$rewardsPoints, totalRewardsPoints=$totalRewardPoints)"
	}

}
