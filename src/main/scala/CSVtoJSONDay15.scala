import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CSVtoJSONDay15 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to JSON Converter")
      .getOrCreate()

    val inputPath = "hdfs://0.0.0.0:9000/employee.csv"
    val outputPath = "hdfs://0.0.0.0:9000/output/employeeJson.json"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    val filteredDf = df.filter(col("Age") < 22)

    filteredDf.write
      .mode("overwrite")
      .json(outputPath)

    val jdbcUrl = "jdbc:mysql://34.132.35.66/testDB"
    val tableName = "employee"
    val properties = new java.util.Properties()
    properties.setProperty("user", "aniket")
    properties.setProperty("password", "aniket")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    println("writing to the database")
    filteredDf.write.mode("overwrite")
      .jdbc(jdbcUrl, tableName, properties)

    spark.stop()
  }
}