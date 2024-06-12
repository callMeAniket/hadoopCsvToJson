import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object s3ToSqlDay14 {
  val config = ConfigFactory.load()
  private val awsAccessKeyId = config.getString("aws.access-key-id")
  private val awsSecretAccessKey = config.getString("aws.secret-access-key")
  private val awsS3Endpoint = config.getString("aws.s3.endpoint")
  private val dbUrl = config.getString("db.url")
  private val dbUser = config.getString("db.user")
  private val dbPassword = config.getString("db.password")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[5]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .appName("Aniket")
      .getOrCreate()

    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.s3a.access.key", awsAccessKeyId)
    hadoopConfig.set("fs.s3a.secret.key", awsSecretAccessKey)
    hadoopConfig.set("fs.s3a.endpoint", awsS3Endpoint)
    hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val schema = StructType(List(
      StructField("id", IntegerType, nullable = true),
      StructField("first_name", StringType, nullable = true),
      StructField("last_name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("position", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true)
    ))

    val employee_df = spark.read.schema(schema).json("s3a://mytestbucket-payoda/employee.json")
    val df_to_sql=employee_df.filter("age>30")
    df_to_sql.write
      .format("jdbc")
      .option("url", dbUrl)
      .option("dbtable", "employees")
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode(SaveMode.Overwrite)
      .save()
    spark.stop()
  }
}