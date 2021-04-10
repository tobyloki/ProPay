

# Troubleshooting
1. Kafka logs failing
    - FIX: delete the kafka logs directory and restart kafka
```
[2021-04-07 20:04:22,084] WARN Stopping serving logs in dir C:\Kafka\kafka_2.13-2.7.0\kafka-logs (kafka.log.LogManager)
[2021-04-07 20:04:22,086] ERROR Shutdown broker because all log dirs in C:\Kafka\kafka_2.13-2.7.0\kafka-logs have failed (kafka.log.LogManager)
```