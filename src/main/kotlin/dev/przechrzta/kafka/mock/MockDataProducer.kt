package dev.przechrzta.kafka.mock

import com.google.gson.GsonBuilder
import dev.przechrzta.kafka.model.StockTransaction
import dev.przechrzta.kafka.window.STOCK_TRANSACTIONS_TOPIC
import mu.KotlinLogging
import org.apache.kafka.clients.producer.*
import java.util.*
import java.util.concurrent.Executors


private val logger = KotlinLogging.logger {}

class MockDataProducer {
	private val gson = GsonBuilder().disableHtmlEscaping().create()
	private var producer: Producer<String, String>? = null
	private val executorService = Executors.newFixedThreadPool(1)
	private var callback: Callback? = null


	fun init() {
		if (producer == null) {
			logger.info { "Initializing producer" }
			val properties = Properties()
			properties["bootstrap.servers"] = "localhost:9092"
			properties["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
			properties["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
			properties["acks"] = "1"
			properties["retries"] = "3"
			producer = KafkaProducer<String, String>(properties)
			callback = Callback { metadata: RecordMetadata?, exception: Exception? ->
				exception?.printStackTrace()
			}
			logger.info { "Producer initialized" }
		}
	}

	fun produceStockTrx(
		numIterations: Int,
		numCompanies: Int,
		numCustomers: Int,
		keyFunc: (StockTransaction) -> String
	) {
		val companies = DataGenerator.generatePublicCompanies(numCompanies)
		val customers = DataGenerator.generateCustomers(numCustomers)

		val task = Runnable {
			init()
			var counter = 0
			while (counter++ < numIterations && keepRunning) {
				val trx = DataGenerator.generateStockTransaction(companies, customers, 50)
				trx.forEach { trx ->
					val value = convertToString(trx)
					val record = ProducerRecord<String, String>(STOCK_TRANSACTIONS_TOPIC, keyFunc(trx), value)
					producer?.send(record)
				}
				logger.info { "Send next trx batch" }
				sleepSomeTime()
			}
			logger.info { "Done trx generation!!!" }
		}

		executorService.submit(task)
	}

	private fun sleepSomeTime() {
		try {
			Thread.sleep(5000)
		} catch (e: InterruptedException) {
			Thread.interrupted()
		}
	}

	companion object {
		var keepRunning = true
	}

	private fun convertToString(trx: StockTransaction): String {
		return gson.toJson(trx)
	}
}

fun main() {
	val generator = MockDataProducer()
	generator.produceStockTrx(2, 5, 5, { it.customerId })
}
