import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object OnlyJoinWithoutGroupBYNestedJsonDay16 extends App {

  val config = ConfigFactory.load()
  private val dbUrl = config.getString("db.url")
  private val dbUser = config.getString("db.user")
  private val dbPassword = config.getString("db.password")
  val spark = SparkSession.builder().master("local[*]").appName("nested_json").getOrCreate()

  val employee_df = spark
    .read
    .format("jdbc")
    .option("url", dbUrl)
    .option("dbtable", "employees2")
    .option("user", dbUser)
    .option("password", dbPassword)
    .load()

  val department_df = spark
    .read
    .format("jdbc")
    .option("url", dbUrl)
    .option("dbtable", "department")
    .option("user", dbUser)
    .option("password", dbPassword)
    .load()

  val joined_df = department_df.join(employee_df, department_df("id") === employee_df("departmentId"), "inner")

  val windowSpec = Window.partitionBy(department_df("id"), department_df("name"))

  private val result_df = joined_df
    .withColumn("employee_names", collect_list(employee_df("first_name")).over(windowSpec))
    .select(department_df("id"), department_df("name"), col("employee_names"))
    .distinct()

  result_df.show()

  result_df.write.mode("overwrite").format("json").save("/Users/aniketsharma/Documents/spark_json/nested_dept_employee.json")
}
