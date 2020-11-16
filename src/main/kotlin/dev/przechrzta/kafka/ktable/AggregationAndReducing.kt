package dev.przechrzta.kafka.aggregation

import dev.przechrzta.kafka.StreamsSerdes
import dev.przechrzta.kafka.common.FixedSizePriorityQueue
import dev.przechrzta.kafka.model.ShareVolume
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import java.text.NumberFormat
import java.util.*

const val STOCK_TRANSACTIONS_TOPIC = "stock-transactions"
const val STOCK_VOLUME_BY_COMPANY = "stock-volume-by-company"
private val logger = KotlinLogging.logger {}

fun main() {
	val stringSerde = Serdes.String()
	val shareVolumeSerde = StreamsSerdes.shareVolume()
	val transactionSerde = StreamsSerdes.stockTransaction()
	val priorityQueueSerde = StreamsSerdes.priorityQueue()

	val numberFormat = NumberFormat.getInstance()

	val comparator = Comparator { s1: ShareVolume, s2: ShareVolume -> s1.shares - s2.shares }
	val queue = FixedSizePriorityQueue(5, comparator)

	val queueToStringMapper = ValueMapper<FixedSizePriorityQueue, String> { fpq ->
		val joiner = StringJoiner(" ")
		fpq.iterator().withIndex()
			.forEach { joiner.add("${it.index}) ${it.value.symbol}: ${numberFormat.format(it.value.shares)}") }
		joiner.toString()
	}

	val builder = StreamsBuilder()
	val shareVolume: KTable<String, ShareVolume> =
		builder.stream(
			STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, transactionSerde)
				.withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
		)
			.mapValues { value -> ShareVolume(value.symbol, value.shares, value.industry) }
			.groupBy({ k: String?, v: ShareVolume -> v.symbol }, Grouped.with(stringSerde, shareVolumeSerde))
			.reduce(ShareVolume.Companion::sum)
//			.toStream()
//			.print(Printed.toSysOut<String,ShareVolume>().withLabel("reduced"))


	val initializer: Initializer<FixedSizePriorityQueue> = Initializer { queue }

	val  aggregatedIndustry = shareVolume.groupBy({ key: String?, value: ShareVolume ->
		KeyValue.pair(value.industry, value)
	}, Grouped.with(stringSerde, shareVolumeSerde))
			.aggregate(
				initializer,
				Aggregator { key: String?, value: ShareVolume, aggregate: FixedSizePriorityQueue ->
					aggregate.add(value)
				},
				Aggregator { key: String?, value: ShareVolume, aggregate: FixedSizePriorityQueue ->
					aggregate.remove(value)
				},
				Materialized.with(stringSerde, priorityQueueSerde))
		.mapValues(queueToStringMapper)
		.toStream().peek{key: String?, value: String? ->
			logger.info{ "---stock volume by industry ${key}, ${value}"}
		}
		aggregatedIndustry.print(Printed.toSysOut<String, String>().withLabel("agg-industry"))
		aggregatedIndustry.to(STOCK_VOLUME_BY_COMPANY, Produced.with(stringSerde, stringSerde))

	val kafkaStreams = KafkaStreams(builder.build(), aggregationProps())
	kafkaStreams.cleanUp()
	kafkaStreams.start()

	Thread.sleep(65000)
	logger.info { "Shutting down kafka streams now" }
	kafkaStreams.close()
}

fun aggregationProps(): Properties {
	val props = Properties()
	props[StreamsConfig.APPLICATION_ID_CONFIG] = "simple-aggregation-app"
	props[ConsumerConfig.GROUP_ID_CONFIG] = "simple-aggregation-group"
	props[ConsumerConfig.CLIENT_ID_CONFIG] = "simple-aggregation-client"
	props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
	props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = "30000"
	props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = "15000"

	props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
	props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = "1"
	props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = "10000"
	props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
	props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

	props[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 1
	props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java

	return props
}
