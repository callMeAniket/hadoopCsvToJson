import org.apache.spark.sql.{DataFrame, SparkSession}

object Day17Modular extends App {

  val spark: SparkSession = createSparkSession()

  val productsPath = "/Users/aniketsharma/Downloads/Products.csv"
  val customersPath = "/Users/aniketsharma/Downloads/Customers.csv"
  val salesPath = "/Users/aniketsharma/Downloads/Sales.csv"

  val products = readCSV(spark, productsPath)
  val customers = readCSV(spark, customersPath)
  val sales = readCSV(spark, salesPath)

  val productsRenamed = renameProductColumns(products)
  val customersRenamed = renameCustomerColumns(customers)
  val salesWithProducts = joinSalesWithProducts(sales, productsRenamed)
  val transactionsWithNames = joinSalesWithCustomers(salesWithProducts, customersRenamed)
  val salesAmountDF = calculateSalesAmount(transactionsWithNames, productsRenamed, customersRenamed)

  salesAmountDF.show()

  writeToCassandra(salesAmountDF, "SalesCFamily", "abhishek_keyspace")

  spark.stop()

  def createSparkSession(): SparkSession = {
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

  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(path)
  }

  def renameProductColumns(products: DataFrame): DataFrame = {
    products.withColumnRenamed("product_id", "prod_id")
  }

  def renameCustomerColumns(customers: DataFrame): DataFrame = {
    customers.withColumnRenamed("customer_id", "cust_id")
      .withColumnRenamed("name", "customer_name")
  }

  def joinSalesWithProducts(sales: DataFrame, products: DataFrame): DataFrame = {
    sales.join(products, sales("product_id") === products("prod_id"))
  }

  def joinSalesWithCustomers(salesWithProducts: DataFrame, customers: DataFrame): DataFrame = {
    salesWithProducts.join(customers, salesWithProducts("customer_id") === customers("cust_id"))
  }

  def calculateSalesAmount(transactionsWithNames: DataFrame, products: DataFrame, customers: DataFrame): DataFrame = {
    transactionsWithNames.select(
      transactionsWithNames("transaction_id"),
      transactionsWithNames("product_id"),
      products("name").as("product_name"),
      customers("customer_name"),
      (transactionsWithNames("units") * products("price")).as("sales_amount")
    ).toDF("transaction_id", "product_id", "product_name", "customer_name", "sales_amount")
  }

  def writeToCassandra(df: DataFrame, table: String, keyspace: String): Unit = {
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .mode("append")
      .save()
  }
}