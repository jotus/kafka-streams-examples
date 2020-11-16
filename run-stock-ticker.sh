#!/usr/bin/env bash

kafkacat -b localhost:9092 -t stock-transactions -T -P -K: -l ./src/main/resources/stock-ticker.json

#for i in {1..30} ; do
#    echo 'AA:{"symbol": "AA", "sector": "SEC-A", "industry": "electr", "shares": 10, "sharePrice": 1.2, "customerId": "cust-1", "purchase": false, "transactionTimestamp" : "2020-05-13T11:32:55.117966+02:00" }' >> ./src/main/resources/stock-ticker.json
#done
