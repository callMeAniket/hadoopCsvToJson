import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

object SparkTask5 {

  private def getDF(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df
  }

  private def getSparkSession: SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("Spark Task 5")
      .master("local[*]")
      .config("spark.cassandra.connection.host","cassandra.ap-south-1.amazonaws.com")
      .config("spark.cassandra.connection.port", "9142")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "aniketProgram-at-533267282416")
      .config("spark.cassandra.auth.password", "dqXefs1niwS9cT4HWSSWyjwpQKiDifh74zTbwYlPPWmvJyUKe4paAlvOHDE=")
      .config("spark.cassandra.input.consistency.level", "LOCAL_QUORUM")
      .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/aniketsharma/cassandra_truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "pbdmbm6*")
      .getOrCreate()
    sparkSession
  }

  def main(args: Array[String]): Unit = {
    try {
      val spark = getSparkSession

      val salesDf = getDF(spark, "/Users/aniketsharma/Downloads/Sales.csv")
      val productsDf = getDF(spark, "/Users/aniketsharma/Downloads/Products.csv").withColumnRenamed("name", "productname")
      val customersDf = getDF(spark, "/Users/aniketsharma/Downloads/Customers.csv").withColumnRenamed("name", "customername")


      val productSalesJoinDf = salesDf.join(productsDf, Seq("product_id"), "inner")

      val productSalesTotalDf = productSalesJoinDf.withColumn("salesamount", col("units")*col("price"))

      val totalPriceCustomersJoinDf = productSalesTotalDf.join(customersDf,
        Seq("customer_id"), "inner") .select(col("transaction_id"), col("product_id"),col("productname"), col("customername"), col("salesamount"))

      totalPriceCustomersJoinDf.show()
      totalPriceCustomersJoinDf.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "SalesCFamily", "keyspace" -> "my_keyspace"))
        .mode("append")
        .save()

    } catch {
      case e: Exception => println(s"Error Occurred: $e")
    }
  }
}