package dev.przechrzta.kafka.model

data class ShareVolume(
	 val symbol: String,
	 val shares: Int = 0,
	 val industry: String
){
	companion object {
		fun sum(s1: ShareVolume, s2: ShareVolume) = s1.copy(shares = s1.shares + s2.shares)
	}
}

