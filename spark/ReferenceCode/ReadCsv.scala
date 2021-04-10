import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.break

object ReadCsv extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Spark shell")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("C:\\Users\\Alex\\Desktop\\FraudPatrol\\sample-data.csv")

//  df2.printSchema()
  df.show(5)
  var firstNames = df.select("first name").collect()

  for(x <- firstNames){
    if(firstNames.indexOf(x) >= 5) break
    println(s"${x(0)} equal to Johnson: ${x(0) == "Johnson"}")
  }
}
