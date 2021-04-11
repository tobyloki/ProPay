# PayPro
For the BeaverHacks Spring 2021
- https://devpost.com/software/propay

ProPay is a real-time credit card processing solution with intelligent fraud detection. Using the latest cutting edge technology bundled with our large-scale backend data, it can process up to petabytes of incoming data and instantly determine whether transactions are fraudulent.

# Run
1. Start Zookeeper in `kafka/batch`
```cmd
zookeeper.bat
```
2. Start Kafka in `kafka/batch`
```cmd
kafka/bat
```
3. Start API (Producer for Kafka) in `website/api`
```cmd
node index.js
```
4. Start WebApp in `website/webapp`
```cmd
npm start
```
5. Start Spark project (Consumer for Kafka)
    1. Open `Consumer` project in IntelliJ
    2. Run `ConsumerStreamingService.scala` in `src/main/scala`

# Data
- `data` folder contains are simulated backend data that will be used by that algorithm to determine whether an incoming transaction is fraudulent

# Testing
- `SampleDataGenerator.scala` in `Consumer` project will generate fake data, acting as producer for Kafka
- `jMeter/generateTestData/index.js` will generate fake data in `test-data.csv` which can be used to load test API using `jMeter/LoadTest.jmx`

# Troubleshooting
1. Kafka logs failing
    - FIX: delete the kafka logs directory and restart kafka
```
[2021-04-07 20:04:22,084] WARN Stopping serving logs in dir C:\Kafka\kafka_2.13-2.7.0\kafka-logs (kafka.log.LogManager)
[2021-04-07 20:04:22,086] ERROR Shutdown broker because all log dirs in C:\Kafka\kafka_2.13-2.7.0\kafka-logs have failed (kafka.log.LogManager)
```