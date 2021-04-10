import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, streaming}

object SparkStreamingConsumerKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val broker_id = "localhost:9092"
    val groupid = "GRP1"
    val topics = "TestTopic"

    val topicset = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("Kafka Demo")

//    val spark = SparkSession.builder()
//      .master("local[*]")
//      .appName("Kafka Demo")
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(/*spark.sparkContext*/sparkconf, streaming.Seconds(2))

    val message = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicset, kafkaParams)
    )
    val words = message.map(_.value()).flatMap(_.split(" "))
    val countwords = words.map(x => (x, 1)).reduceByKey(_+_)
    countwords.print()

    ssc.start()
    ssc.awaitTermination()
  }
}