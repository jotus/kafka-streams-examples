package dev.przechrzta.kafka.model

class RewardAccumulator (
	val customerId: String,
	val purchaseTotal: Double,
	rewardsPoints: Int
) {

	var currentRewardPoints: Int = rewardsPoints
	var totalRewardPoints: Int = rewardsPoints
	private var daysFromLastPurchase: Int = 0


	fun addRewardPoints(previousTotalPoints: Int) {
		this.totalRewardPoints += previousTotalPoints
	}
}
