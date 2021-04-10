import org.apache.spark.sql.SparkSession

object ReadFile extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkSession")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("C:\\Users\\Alex\\Desktop\\DialCodes\\DialAwsTest\\GenerateUsers\\users.csv")
  println(rddFromFile.getClass)

  val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

  rdd.mapPartitionsWithIndex {
    (idx, iter) => if (idx == 0) iter.drop(1) else iter
  }

  println("Iterate RDD")
  rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  println(rdd)

  println("Get data Using collect")
  rdd.collect().foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
}
