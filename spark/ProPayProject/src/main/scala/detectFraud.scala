import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate

object SparkSQLDataset {

  case class Card(ID:String,card:String, expiration:String,cvv:String,zip:String)
  case class FraudHist(ID:String, cardID:String, Detail:String)
  case class CriminalHist(ID:String, cardID:String, Detail:String)
  case class ChargeHist(ID:String, cardID:String, Detail:String)
  case class AdvCard(ID:String,cardID:String, firstName:String,
                     lastName:String,phone:String,email:String,credit:String)
  case class CardAddress(ID:String, shipAddress:String,shipCity:String,shipState:String,
                         shipZip:String,shipCountry:String,billAddress:String,
                         billCity:String,billState:String,billZip:String,billCountry:String)

  case class FullData(nameOnCard:String, card:String, cvv:String, expiration:String, phone:String, email:String,billingName:String,
                      billingAddress:String, billingAddress2:String,billingCountry:String, billingState:String,
                      billingZip:String, shippingAddress:String, shippingAddress2:String,shippingCountry:String, shippingState:String, shippingZip:String,
                      credit:String,DetailHistory:String,DetailCriminalHist:String,DetailFraud:String)

  case class CompleteData(ID: String,
                          cardID:String, cardNumber: String, expiration:String,cvv:String,zip:String,
                          fraudDetail: String, criminalDetail: String, chargeDetail: String,
                          firstname:String, lastname: String,phone:String, email:String,
                          shipAddress:String,shipCity:String,shipState:String,
                          shipZip:String,shipCountry:String,billAddress:String,
                          billCity:String,billState:String,billZip:String,billCountry:String,
                          var fraudScore:Integer, var isValidTransaction:Boolean
                         )

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    // Test Cases:

    //    val data1 = Seq(FraudHist("1","1",""),
    //      FraudHist("5","5",""),
    //      FraudHist("9","9",""))
    //    val data2 = Seq(Card("1","1234123412341232","2021-06-13","123","12345"),
    //              Card("5","1234123412341234","2021-06-13","123","12345"),
    //              Card("9","5555555555554444","2021-06-13","123","12345"))
    //    val data3 = Seq(AdvCard("1","1","Person","1","1234567890","myemail","450"),
    //              AdvCard("5","5","Person","2","1234567890","myemail","500"),
    //              AdvCard("9","9","Person","3","1234567890","myemail","720"))
    //    val data4 = Seq(CardAddress("1","123 Street St NE","","OR","12345","US","123 Street St NE","","OR","12345","US"),
    //              CardAddress("5","123 Street St NE","","OR","12345","US","123 Street St NE","","OR","12345","US"),
    //              CardAddress("9","123 Street St NE","","OR","12345","US","123 Street St NE","","OR","12345","US"))
    //    val spark = SparkSession
    //          .builder
    //          .appName("SparkSQL")
    //          .master("local[*]")
    //          .getOrCreate()
    //    import spark.implicits._
    //    val ds1: Dataset[FraudHist] = data1.toDS()
    //    val ds2: Dataset[Card] = data2.toDS()
    //    val ds3: Dataset[AdvCard] = data3.toDS()
    //    val ds4: Dataset[CardAddress] = data4.toDS()

    // fails basic test
    val test1 = FullData("Person 1", "1234123412341234", "123", "06/21", "1234567890", "myemail", "",
      "123 Street St NE", "", "US", "OR", "12345",
      "123 Street St NE", "", "US", "OR", "12345",
      "","","","")
    // passes basic but fails advanced
    val test2 = FullData("", "5555555555554444", "123", "06/21", "0", "", "",
      "123 Street St NE", "", "US", "OR", "12345",
      "123 Street St NE", "", "UAE", "OR", "12345",
      "","","","")
    // passes advanced
    val test3 = FullData("Person 1", "1234123412341232", "123", "06/21", "1234567890", "myemail", "",
      "123 Street St NE", "", "US", "OR", "12345",
      "123 Street St NE", "", "US", "OR", "12345",
      "","","","")
    
    println(detectFraud(test1))
    println(detectFraud(test2))
    println(detectFraud(test3))

  }

  def detectFraud(row: FullData): (Integer,Boolean) ={ //takes a row
    //Basic Test
    val currentDate = LocalDate.now()
    val expirationDate = "20" + row.expiration.substring(3) + "-" + row.expiration.substring(0, 2) + "-01"
    println(expirationDate)
    if(row.card == "" ||
      !luhnAlgorithm(row.card.toLong) || //verify valid card number
      row.cvv.toInt >= 1000 || row.cvv.toInt <= 99 || //verify cvv is 3 digits
      !validAddress(row.billingAddress) || //verify valid address
      row.billingState == "" ||
      row.billingCountry == "" ||
      row.billingZip.toInt >= 100000 || row.billingZip.toInt <= 9999 || //verify zip is 5 digits
      row.expiration == "" ||
      LocalDate.parse(expirationDate).compareTo(currentDate) < 0 //verify not expired
    ){
      return (0,false)
    }
    //Advanced Test
    var score = 80
    if(row.nameOnCard != ""){
      score += 5
    }
    if(row.phone.toInt > 999999999 && row.phone.toInt < 10000000000L){ //10 digits
      score += 5
    }
    if(row.email != ""){
      score += 5
    }
    //    if(row.credit.toInt >= 550 && row.credit.toInt <= 850){
    //      score += 5
    //    } else if (row.credit.toInt >= 500) {
    //      score += 4
    //    } else if (row.credit.toInt >= 450) {
    //      score += 3
    //    } else if (row.credit.toInt >= 400) {
    //      score += 2
    //    } else if (row.credit.toInt >= 350) {
    //      score += 1
    //    } //else no credit score given (or invalid) or very bad score (min. 300)
    if(row.shippingCountry != "" &&
      row.shippingCountry != row.billingCountry){
      score -= 5
    }
    //    if(row.DetailHistory != ""){
    //      score -= 10
    //    }
    //    if(row.DetailCriminalHist != ""){
    //      score -= 10
    //    }
    //    if(row.DetailFraud != ""){
    //      score /= 2
    //    }
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



  def findFraud(ds1: Dataset[Card], ds2: Dataset[FraudHist], ds3: Dataset[CriminalHist], ds4: Dataset[ChargeHist],
                ds5: Dataset[AdvCard], ds6: Dataset[CardAddress]): Dataset[CompleteData] ={
    //merge six datasets into one
    val transactionData = ds1.join(ds2, "cardID").join(ds3, "cardID").join(ds4, "cardID").join(ds5, "cardID").join(ds6, "cardID")
    //feed each row of the new dataset into the algorithm
    var data: Seq[CompleteData] = List()
    for(i <- 0 to transactionData.count().toInt - 1){
      //get row from merged data set
      val row = transactionData.take(i+1).drop(i)
      //convert to Copmplete class
      var myRow: FullData = FullData(
        i.toString,                             //ID

        row(0).get(0).asInstanceOf[String],     //cardID
        row(0).get(1).asInstanceOf[String],     //card number
        row(0).get(2).asInstanceOf[String],     //expiration
        row(0).get(3).asInstanceOf[String],     //cvv
        row(0).get(4).asInstanceOf[String],     //zip

        row(0).get(6).asInstanceOf[String],     //detail fraud history
        row(0).get(8).asInstanceOf[String],     //detail criminal history
        row(0).get(10).asInstanceOf[String],    //detail charge back history

        row(0).get(11).asInstanceOf[String],    //first name
        row(0).get(12).asInstanceOf[String],    //last name
        row(0).get(13).asInstanceOf[String],    //phone
        row(0).get(18).asInstanceOf[String],    //email
        row(0).get(19).asInstanceOf[String],    //credit
        row(0).get(20).asInstanceOf[String],    //ship address
        row(0).get(21).asInstanceOf[String],    //ship city
        row(0).get(22).asInstanceOf[String],    //ship state
        row(0).get(23).asInstanceOf[String],    //ship zip
        row(0).get(24).asInstanceOf[String],    //ship country
        row(0).get(25).asInstanceOf[String],    //bill address
        row(0).get(26).asInstanceOf[String],    //bill city
      )
      val results = detectFraud(myRow)
      //append two values returned by algorithm
      //      myRow.fraudScore = results._1
      //      myRow.isValidTransaction = results._2
      //append new row to the array
      //      data = data :+ myRow
    }
    //create dataset from the array
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val newDS: Dataset[CompleteData] = data.toDS()
    //return new dataset
    return newDS
  }

}