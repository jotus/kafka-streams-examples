package dev.przechrzta.kafka.mock

import com.github.javafaker.Faker
import dev.przechrzta.kafka.model.PublicCompany
import dev.przechrzta.kafka.model.StockTransaction
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import java.util.regex.Pattern

object DataGenerator {
	val dateFaker = Faker()
	private var timestampGenerator: Supplier<Date> =
		Supplier {
			dateFaker.date().past(15, TimeUnit.MINUTES, Date())
		}

	fun generatePublicCompanies(num: Int): List<PublicCompany> {
		val symbols = listOf("AEBB", "VABC", "ALBC", "EABC", "BWBC", "BNBC", "MASH", "BARX", "WNBC", "WKRP")
		val companyName = listOf(
			"Acme Builders",
			"Vector Abbot Corp",
			"Albatros Enterprise",
			"Enterprise Atlantic",
			"Bell Weather Boilers",
			"Broadcast Networking",
			"Mobile Surgical",
			"Barometer Express",
			"Washington National Business Corp",
			"Cincinnati Radio Corp."
		)

		val faker = Faker()
		val random = Random()
		val companies = mutableListOf<PublicCompany>()
		(1..num).forEach {
			val name: String = faker.company().name()
			val stripped = name.replace("[^A-Za-z]".toRegex(), { "" })
			val start = random.nextInt(stripped.length - 4)
			val symbol = stripped.subSequence(start, start + 4) as String

			val lastSold = faker.number().randomDouble(2, 15, 150)
			val sector = faker.options().option("Energy", "Finance", "Technology", "Transportation", "Health Care")
			val industry = faker.options().option(
				"Oil & Gas Production",
				"Coal Mining",
				"Commercial Banks",
				"Finance/Investors Services",
				"Computer Communications Equipment",
				"Software Consulting",
				"Aerospace",
				"Railroads",
				"Major Pharmaceuticals"
			)
			val company = PublicCompany(lastSold, symbol, name, sector, industry)
			companies.add(company)
		}



		return companies
	}


	private fun generateCreditCardNumbers(numberCards: Int): List<String> {
		var counter = 0
		val visaMasterCardAmex = Pattern.compile("(\\d{4}-){3}\\d{4}")
		val creditCardNumbers: MutableList<String> =
			ArrayList(numberCards)
		val finance = Faker().finance()
		while (counter < numberCards) {
			val cardNumber = finance.creditCard()
			if (visaMasterCardAmex.matcher(cardNumber).matches()) {
				creditCardNumbers.add(cardNumber)
				counter++
			}
		}
		return creditCardNumbers
	}


	fun generateCustomers(num: Int): List<Customer> {

		val faker = Faker()

		val customers = mutableListOf<Customer>()
		val creditCards = generateCreditCardNumbers(num)
		(1..num).forEach {
			val name = faker.name()
			val customerId = faker.idNumber().valid()
			val creditCard = creditCards.get(it - 1)
			customers.add(Customer(name.firstName(), name.lastName(), customerId, creditCard))
		}
		return customers
	}

	fun generateStockTransaction(
		companies: List<PublicCompany>,
		customers: List<Customer>,
		num: Int
	): List<StockTransaction> {
		val faker = Faker()
		val transactions = mutableListOf<StockTransaction>()
		(1..num).forEach {
			val numberShares = faker.number().numberBetween(100, 50000);
			val company = companies.get(faker.number().numberBetween(0, companies.size))
			val customer = customers.get(faker.number().numberBetween(0, customers.size))
			val timestamp = timestampGenerator.get()
			val transaction = StockTransaction(
				company.symbol,
				company.sector,
				company.industry,
				customer.customerId,
				numberShares,
				company.updateStockPrice(),
				timestamp,
				true
			)
			transactions.add(transaction)
		}
		return transactions
	}
}

fun main() {
	val  companies = DataGenerator.generatePublicCompanies(10)
	val  customers = DataGenerator.generateCustomers(3)
	val trx = DataGenerator.generateStockTransaction(companies, customers, 10)
	println(trx)
}

data class Customer(val firstName: String, val lastName: String, val customerId: String, val creditCardNum: String)
