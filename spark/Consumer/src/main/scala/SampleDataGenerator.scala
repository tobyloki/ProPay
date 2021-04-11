import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

/** Count up how many of each word occurs in a book, using regular expressions. */
object SampleDataGenerator {


  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val t = "{\"nameOnCard\":\"jon\",\"card\":\"654654654\",\"cvv\":\"444\",\"expiration\":\"06/97\",\"phone\":\"5558794\",\"email\":\"test@em.com\",\"billingName\":\" bilname\",\"billingAddress\":\"biladdr\",\"billingAddress2\":\"billaddr\",\"billingCity\":\"bilcty\",\"billingCountry\":\"bilcntry\",\"billingState\":\"bilst\",\"billingZip\":\"billzip\",\"shippingAddress\":\"shipaddr\",\"shippingAddress2\":\"shipaddr2\",\"shippingCity\":\"shipcty\",\"shippingCountry\":\"shipctry\",\"shippingState\":\"shipst\",\"shippingZip\":\"shipzip\"}"
    val topic = "TestTopic"
    while(1==1){
      try {
        val obj = new ProducerRecord[String, String](topic, t.toString()) // refer to: https://sparkbyexamples.com/kafka/apache-kafka-consumer-producer-in-scala/#Producer-Program
        Thread.sleep(5000)
        val metadata = producer.send(obj)
        printf(s"sent obj(key=%s value=%s) " +"meta(partition=%d, offset=%d)\n",obj.key(), obj.value(),metadata.get().partition(),metadata.get().offset())
      }catch{
        case e:Exception => e.printStackTrace()
      }
    }

  }

}

