package dev.przechrzta.kafka.model

data class TransactionSummary(
	 val customerId: String,
	 val stockTicker: String,
	 val industry: String,
	 val summaryCount: Long = 0,
	 val customerName: String? = null,
	 val companyName: String? = null
){
	companion object{
		fun fromTransaction(trx: StockTransaction): TransactionSummary{
			return TransactionSummary(trx.customerId, trx.symbol, trx.industry)
		}
	}
}
