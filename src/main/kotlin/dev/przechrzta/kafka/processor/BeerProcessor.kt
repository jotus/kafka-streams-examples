package dev.przechrzta.kafka.processor

import dev.przechrzta.kafka.model.BeerPurchase
import dev.przechrzta.kafka.model.Currency
import org.apache.kafka.streams.processor.AbstractProcessor


class BeerProcessor(val domestinName: String, val interName: String)
	: AbstractProcessor<String, BeerPurchase>(){
	override fun process(key: String?, value: BeerPurchase) {

		if (value.currency == Currency.DOLLAR) {
			val next = value.copy(currency = Currency.EURO, totalSale = 10.0)
			context().forward(key,next, interName)
		} else {
			context().forward(key, value, domestinName)
		}
	}
}
