import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class WriteToKeyspacesTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("Test WriteToKeyspaces")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Test createSparkSession") {
    assert(spark.isInstanceOf[SparkSession])
  }

  test("Test readCSV") {
    val df = Day17Modular.readCSV(spark, "src/test/resources/Products.csv")
    assert(df.columns.contains("product_id"))
    assert(df.columns.contains("name"))
  }

  test("Test renameProductColumns") {
    val products = Day17Modular.readCSV(spark, "src/test/resources/Products.csv")
    val renamedProducts = Day17Modular.renameProductColumns(products)
    assert(renamedProducts.columns.contains("prod_id"))
    assert(!renamedProducts.columns.contains("product_id"))
  }

  test("Test renameCustomerColumns") {
    val customers = Day17Modular.readCSV(spark, "src/test/resources/Customers.csv")
    val renamedCustomers = Day17Modular.renameCustomerColumns(customers)
    assert(renamedCustomers.columns.contains("cust_id"))
    assert(!renamedCustomers.columns.contains("customer_id"))
    assert(renamedCustomers.columns.contains("customer_name"))
    assert(!renamedCustomers.columns.contains("name"))
  }

  test("Test joinSalesWithProducts") {
    val sales = Day17Modular.readCSV(spark, "src/test/resources/Sales.csv")
    val products = Day17Modular.readCSV(spark, "src/test/resources/Products.csv")
    val renamedProducts = Day17Modular.renameProductColumns(products)
    val result = Day17Modular.joinSalesWithProducts(sales, renamedProducts)
    assert(result.columns.contains("prod_id"))
  }

  test("Test joinSalesWithCustomers") {
    val sales = Day17Modular.readCSV(spark, "src/test/resources/Sales.csv")
    val products = Day17Modular.readCSV(spark, "src/test/resources/Products.csv")
    val customers = Day17Modular.readCSV(spark, "src/test/resources/Customers.csv")
    val renamedProducts = Day17Modular.renameProductColumns(products)
    val renamedCustomers = Day17Modular.renameCustomerColumns(customers)
    val salesWithProducts = Day17Modular.joinSalesWithProducts(sales, renamedProducts)
    val result = Day17Modular.joinSalesWithCustomers(salesWithProducts, renamedCustomers)
    assert(result.columns.contains("cust_id"))
  }

  test("Test calculateSalesAmount") {
    val sales = Day17Modular.readCSV(spark, "src/test/resources/Sales.csv")
    val products = Day17Modular.readCSV(spark, "src/test/resources/Products.csv")
    val customers = Day17Modular.readCSV(spark, "src/test/resources/Customers.csv")
    val renamedProducts = Day17Modular.renameProductColumns(products)
    val renamedCustomers = Day17Modular.renameCustomerColumns(customers)
    val salesWithProducts = Day17Modular.joinSalesWithProducts(sales, renamedProducts)
    val transactionsWithNames = Day17Modular.joinSalesWithCustomers(salesWithProducts, renamedCustomers)
    val result = Day17Modular.calculateSalesAmount(transactionsWithNames, renamedProducts, renamedCustomers)
    assert(result.columns.contains("sales_amount"))
  }
}