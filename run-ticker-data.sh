kafkacat -b localhost:9092 -t stock-ticker-stream -T -P -K: -l ./src/main/resources/ticker-data.json
kafkacat -b localhost:9092 -t stock-ticker-table -T -P -K: -l ./src/main/resources/ticker-data.json
