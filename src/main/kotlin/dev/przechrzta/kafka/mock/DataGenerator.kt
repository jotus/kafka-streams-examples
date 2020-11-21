package dev.przechrzta.kafka.mock

import com.github.javafaker.Faker
import dev.przechrzta.kafka.model.PublicCompany
import java.util.*

object DataGenerator {
//	  val faker = Faker()

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
		(1..num).forEach{
			val name: String = faker.company().name()
			val stripped = name.replace("[^A-Za-z]".toRegex(), { ""})
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
}

fun main() {
	println(DataGenerator.generatePublicCompanies(10))
}
