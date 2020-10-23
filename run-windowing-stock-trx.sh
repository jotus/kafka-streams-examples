kafkacat -b localhost:9092 -t stock-transactions -T -P -l ./src/main/resources/stock-transactions-windowing.json
