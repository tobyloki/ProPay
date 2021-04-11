import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{BufferedWriter, File, FileWriter}
import java.time.LocalDate
import java.util.Properties


// proucer-generator is in sbt project, in file: WordCountsBetter.scala

// i am returned  1 row, score (int), fraud (Bool), all prev fields
// append this to out.csv
object SparkStreamingConsumerKafka {
  case class Data(name:String, card:Int, cvv:Int, expiration:String, billingAddress:String, billingCountry:String, billingState:String,
                  billingZip:String, shippingAddress:String, shippingCountry:String, shippingState:String, shippingZip:String)
  case class FullData(nameOnCard:String, card:String, cvv:String, expiration:String, phone:String, email:String,billingName:String,
                      billingAddress:String, billingAddress2:String,billingCity:String, billingCountry:String, billingState:String,
                      billingZip:String, shippingAddress:String, shippingAddress2:String,shippingCity:String, shippingCountry:String, shippingState:String, shippingZip:String)
  // phone, email, billname, billaddr2, shippaddr2 not used perhaps, according to Alex
  case class FraudHist(ID:Int, cardID:Int, Detail:String)
  case class CriminalHist(ID:Int, cardID:Int, Detail:String)
  case class ChargeHist(ID:Int, cardID:Int, Detail:String)
  case class Card(ID:String,card:String, expiration:String,cvv:String,zip:String)
  case class AdvCard(ID:String,cardID:String, firstName:String,
                     lastName:String,phone:String,email:String,credit:String)
  case class CardAddress(ID:String, shipAddress:String,shipCity:String,shipState:String,
                         shipZip:String,shipCountry:String,billAddress:String,
                         billCity:String,billState:String,billZip:String,billCountry:String)
  // nameOnCard,card,cvv,expiration,phone,email,billingName,billingAddress,billingAddress2,billingCity,billingCountry,billingState,billingZip,shippingAddress,shippingAddress2,shippingCity,shippingCountry,shippingState,shippingZip
  // billingcity, shippingcity, after each addr2
  def FraudHistMapper(line:String): FraudHist = {
    val fields = line.split(',')
    FraudHist(fields(0).toInt, fields(1).toInt, fields(2))
  }
  def CriminalHistMapper(line:String): CriminalHist = {
    val fields = line.split(',')
    CriminalHist(fields(0).toInt, fields(1).toInt, fields(2))
  }
  def ChargeHistMapper(line:String): ChargeHist = {
    val fields = line.split(',')
    ChargeHist(fields(0).toInt, fields(1).toInt, fields(2))
  }
  def CardMapper(line:String): Card = {
    val fields = line.split(',')
    val cardnum = fields(1).replaceAll("\\s","").replaceAll("-","")
    val datestr = fields(2).split('/')
    var date = datestr(0)+"/"
    if (datestr(0).toInt<10){
      date = "0"+datestr(0)+"/"
    }
    date = date + datestr(2).charAt(datestr(2).length-2) + datestr(2).charAt(datestr(2).length-1)
    //val expdate = oldDateFormat.parse(fields(2))
    Card(fields(0), cardnum, date/*expDateFormat.format(expdate)*/, fields(3), fields(4))
  }
  def AdvCardMapper(line:String): AdvCard = {
    val fields = line.split(',')
    AdvCard(fields(0), fields(1), fields(2), fields(3), fields(4).replaceAll("-",""),fields(5),fields(6))
  }
  def CardAddrMapper(line:String): CardAddress = {
    val fields = line.split(',')
    CardAddress(fields(0),fields(1),fields(2),fields(3),fields(4),fields(5),fields(6),fields(7),fields(8),fields(9),fields(10))
  }
  // adam uses phone and email
  def DataMapper(line:String): Data={
    val fields = line.split(',')
    Data(fields(0), fields(1).toInt, fields(2).toInt, fields(3), fields(4),
      fields(5), fields(6), fields(7), fields(8), fields(9), fields(10), fields(11))
  }
  def FullDataMapper(line:String): FullData={

    val fields = line.split(',')
    // debug
    //      println("data is: "+line);
    //      println("len of line is: "+fields.length)
    //      println("Line:");
    // fields.foreach(println)
    // debug over
    FullData(fields(0), fields(1), fields(2), fields(3), fields(4).replaceAll("-",""),
      fields(5), fields(6), fields(7), fields(8), fields(9), fields(10), fields(11), fields(12), fields(13), fields(14), fields(15), fields(16), fields(17),fields(18))
  }

  def parseData(line:String):String={
    implicit val formats = org.json4s.DefaultFormats//+ new AddressSerializer
    val p = parse(line).extract[Data]
    p.name+","+p.card+","+p.cvv+","+p.expiration+","+p.billingAddress+","+p.billingCountry+","+p.billingState+","+p.billingZip+","+p.shippingAddress+","+p.shippingCountry+","+p.shippingState+","+p.shippingZip
  }
  def parseFullData(line:String):String={
    implicit val formats = org.json4s.DefaultFormats //+ new AddressSerializer
    val p = parse(line).extract[FullData]
    p.nameOnCard+","+p.card+","+p.cvv+","+p.expiration+","+p.phone+","+p.email+","+p.billingName+","+p.billingAddress+","+p.billingAddress2+","+p.billingCity+","+p.billingCountry+","+p.billingState+","+p.billingZip+","+p.shippingAddress+","+p.shippingAddress2+","+p.shippingCity+","+p.shippingCountry+","+p.shippingState+","+p.shippingZip
  }

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
    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "rowStream"
    val spark = SparkSession.builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


    // Fraud History
    val fraud_rows = spark.sparkContext.textFile("data/CardFraudHistory.csv")
    val mapped_fraud_rows = fraud_rows.map(FraudHistMapper)
    val ds2: Dataset[FraudHist] = mapped_fraud_rows.toDS()
    // Chargeback History
    val chargeback_rows = spark.sparkContext.textFile("data/CardChargebackHistory.csv")
    val mapped_chargeback_rows = chargeback_rows.map(ChargeHistMapper)
    val ds3: Dataset[ChargeHist] = mapped_chargeback_rows.toDS()
    // Criminal History
    val criminal_rows = spark.sparkContext.textFile("data/CardCriminalHistory.csv")
    val mapped_criminal_rows = criminal_rows.map(CriminalHistMapper)
    val ds4: Dataset[CriminalHist] = mapped_criminal_rows.toDS()
    // cardBasic
    val card_rows = spark.sparkContext.textFile("data/CardBasic.csv")
    val mapped_card_rows = card_rows.map(CardMapper)
    val ds5: Dataset[Card] = mapped_card_rows.toDS()
    // cardAdvanced
    val advcard_rows = spark.sparkContext.textFile("data/CardAdvanced.csv")  // change folder
    val mapped_advcard_rows = advcard_rows.map(AdvCardMapper)
    val ds6: Dataset[AdvCard] = mapped_advcard_rows.toDS()
    // cardAddressRelationships
    val cardAddress_rows = spark.sparkContext.textFile("data/CardAddressRelationships.csv")  // change folder
    val mapped_cardAddress_rows = cardAddress_rows.map(CardAddrMapper)
    val ds7: Dataset[CardAddress] = mapped_cardAddress_rows.toDS()

    val ssc = new StreamingContext(spark.sparkContext, streaming.Milliseconds(2000)) // every 0.5s
    val message = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicset, kafkaParams)
    )
    val inputs = message.map(_.value())
    def SendOut(row:Row): Unit = {
      try {
        val obj = new ProducerRecord[String, String](topic, row.toString()) // refer to: https://sparkbyexamples.com/kafka/apache-kafka-consumer-producer-in-scala/#Producer-Program
        val metadata = producer.send(obj)
      }catch{
        case e:Exception => e.printStackTrace()
      }
    }
    def SendResponse (row: FullData): Unit ={
      val results = detectFraud(row)
      val output = row.nameOnCard+","+row.card+","+row.cvv+","+row.expiration+","+row.phone+","+row.email+","+row.billingName+","+row.billingAddress+","+row.billingAddress2+","+row.billingCity+","+ row.billingCountry+","+row.billingState+","+row.billingZip+","+row.shippingAddress+","+row.shippingAddress2+","+row.shippingCity+","+row.shippingCountry+","+ row.shippingState+","+row.shippingZip+","+results._1+","+results._2+"\n"
      val writeFile = new File("output/data.csv")
      val writer = new BufferedWriter(new FileWriter(writeFile, true))
      writer.write(output)
      writer.close()
    }
    def RDDMapper(rdd:RDD[String]): RDD[String] = {
      val parsed_rdd = rdd.map(parseFullData) // change this to other one
      val rows = parsed_rdd.map(FullDataMapper) // change this to other one
      import spark.implicits._
      val schema = rows.toDS // adam's code comes in here
      val dataArr = schema.collect();
      dataArr.foreach(x=>SendResponse(x))

      //schema.foreach(x=>SendResponse(x))
      /*
      schema.createOrReplaceTempView("cards")
      val select_all = spark.sql("SELECT * FROM cards")
      val results = select_all.collect()
      results.foreach(println)
      results.foreach(SendOut)
       */
      rdd
    }
    inputs.foreachRDD(x => RDDMapper(x))
    val Sent = inputs.map(x=> ("count",1)).reduceByKey(_+_)
    Sent.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def detectFraud(row: FullData): (Integer,Boolean) ={ //takes a row
    //Basic Test
    println("row: "+row.toString)
    val currentDate = LocalDate.now()
    val expirationDate = "20" + row.expiration.substring(3) + "-" + row.expiration.substring(0, 2) + "-01"
    if(row.card == "" || !luhnAlgorithm(row.card.toLong)  //verify valid card number
      || row.cvv.toInt >= 1000  || row.cvv.toInt <= 99  //verify cvv is 3 digits
      || !validAddress(row.billingAddress)  //verify valid address
      || row.billingState == ""
      || row.billingCountry == ""
      || row.billingZip.toInt >= 100000  || row.billingZip.toInt <= 9999  //verify zip is 5 digits
      || row.expiration == ""
      || LocalDate.parse(expirationDate).compareTo(currentDate) < 0 //verify not expired
    ){
      return (0,false)
    }
    //Advanced Test
    var score = 80
    if(row.nameOnCard != ""){
      score += 5
    }
    if(row.phone.toInt > 999999999 && row.phone.toInt < 10000000000L){ //9 digits
      score += 5
    }
    if(row.email != ""){
      score += 5
    }
    if(row.shippingCountry != "" &&
      row.shippingCountry != row.billingCountry){
      score -= 5
    }
    return (score,score>=80) //if less than 80, probably fraud
  }

  def luhnAlgorithm(cardNumberInt: Long): Boolean = {
    val cardNumberStr = cardNumberInt.toString //convert to string
    if(cardNumberStr.length != 16){ //confirm 16 digit length
      return false
    }
    // set up helper variables
    var cardSum: Integer = 0
    var i: Integer = 0
    // loop through each digit
    for( i <- 0 to 15){
      var digit: Integer = cardNumberStr.charAt(i).toInt - '0' // get integer value of digit
      if(i % 2 == 1){  // if odd numbered digit, double its value
        digit = digit * 2
      }
      // add digit to sum
      cardSum = cardSum + digit / 10
      cardSum = cardSum + digit % 10
    }
    return (cardSum % 10 == 0)
  }

  def validAddress(addressStr: String): Boolean = {
    val components = addressStr.split(" ")
    if(components.length == 4 && //exactly four elements
      isAllDigits(components(0)) &&//first element is the address number
      components(1) != "" && //street name is not none
      components(2) != "" //street type is not none
    ){
      val cardinals = List("N", "NE", "E", "SE", "S", "SW", "W", "NW")
      var validCardinal = false
      for(i <- 0 to 7) {
        if(components(3) == cardinals(i)){ //valid cardinal direction
          validCardinal = true
        }
      }
      if(validCardinal) {
        return true
      }
    }
    return false
  }
  def isAllDigits(x: String) = x forall Character.isDigit
}
