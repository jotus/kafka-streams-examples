kafkacat -C -b localhost:9092 -t stock-ticker-stream -K: -f 'Key: %k\nValue: %s\n'
kafkacat -C -b localhost:9092 -t stock-ticker-table -K: -f 'Key: %k\nValue: %s\n'
