import org.apache.spark.sql.{SaveMode, SparkSession}
import s3ToSqlDay14.config

object SqlToS3Day14 {
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
    val employee_df = spark
      .read
      .format("jdbc")
      .option("url", dbUrl)
      .option("dbtable", "employees")
      .option("user", dbUser)
      .option("password", dbPassword)
      .load()
    val filtered_df = employee_df.filter("position='Project Manager'")
    filtered_df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("s3a://mytestbucket-payoda/project_manager/")
    spark.stop()
  }
}
