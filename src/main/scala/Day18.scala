import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day18 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("practice").getOrCreate()
    val path = "/Users/aniketsharma/Downloads/Sales.csv"

    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .format("csv")
      .load(path)

    df.show() //  to print

    df.printSchema() //  schema of the df

    df.describe().show() //  describe , basically the aggregate functions, sum,count, max

    df.select("transaction_id", "customer_id") // select specific
      .show()

    // Add a new column with a constant value
    val dfWithNewColumn = df.withColumn("new_column", lit(100))
    dfWithNewColumn
      .withColumnRenamed("new_column", "newColumn") // column name changed from new_column to newColumn
      .show()

    dfWithNewColumn
      .drop("newColumn")
      .show()

    df.filter(col("transaction_id") > 4)
      .show()

    df.sort(col("units").desc)
      .show()

    df.orderBy(col("units").asc)
      .show()

    df.groupBy(col("product_id")).count()
      .show()

    import spark.implicits._

    // Sample DataFrames
    val df1 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val df2 = Seq((1, "NY"), (2, "LA")).toDF("id", "city")

    // Join Operations
    val joinedDf = df1.join(df2, df1("id") === df2("id"))
    joinedDf.show()

    val outerJoinedDf = df1.join(df2, Seq("id"), "outer")
    outerJoinedDf.show()

    // Set Operations
    val unionDf = df1.union(df2)
    unionDf.show()

    val intersectDf = df1.intersect(df2)
    intersectDf.show()

    val exceptDf = df1.except(df2)
    exceptDf.show()

    // Miscellaneous Operations
    val df3 = Seq((1, Array("a", "b", "c"))).toDF("id", "arrayCol")
    val explodedDf = df3.withColumn("exploded", explode($"arrayCol"))
    explodedDf.show()

    // Dummy value column for the pivot example
    val df4 = Seq((1, "a", 1), (1, "b", 2), (1, "c", 3)).toDF("id", "arrayCol", "valueColumn")
    val pivotDf = df4.groupBy("id").pivot("arrayCol").agg(sum("valueColumn"))
    pivotDf.show()

    // Window Functions
    val windowSpec = Window.partitionBy("id").orderBy("id")
    val cumSumDf = df3.withColumn("cumSum", sum("id").over(windowSpec))
    cumSumDf.show()

    // Input/Output
    df3.write.format("json").save("path/to/json2")

    // Dataset Operations
    val data1 = Seq(1, 2, 3, 4, 5)
    val dataset1 = spark.createDataset(data1)
    val jsonDataset = spark.read.json("path/to/json2")
    val mappedDataset = dataset1.map(_ * 2)
    val flatMappedDataset = dataset1.flatMap(x => Seq(x, x * 2))
    val filteredDataset = dataset1.filter(_ > 2)
    val distinctDataset = dataset1.distinct()
    val sampledDataset = dataset1.sample(false, 0.5)
    val unionDataset = dataset1.union(dataset1)
    val joinedDataset = dataset1.join(dataset1, dataset1("value") === dataset1("value"))
    val groupedDataset = dataset1.groupBy("value")

    // Dataset Actions
    val collectedData = dataset1.collect()
    val count1 = dataset1.count()
    val firstElement = dataset1.first()
    val firstNElements = dataset1.take(5)
    dataset1.show()

    // Dataset Persistence
    dataset1.cache()
    dataset1.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    // Dataset Partitioning
    val repartitionedDataset = dataset1.repartition(10)
    val coalescedDataset = dataset1.coalesce(1)

    // File Output
    dataset1.write.format("parquet").save("path/to/parquet1")

    // Sample DataFrames
    val df_1 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    val df_2 = Seq((1, "NY"), (2, "LA")).toDF("id", "city")

    // DataFrame Join Operations
    val joined_df = df_1.join(df_2, df_1("id") === df_2("id"))
    val outer_joined_df = df_1.join(df_2, Seq("id"), "outer")

    // DataFrame Set Operations
    val union_df = df_1.union(df_2)
    val intersect_df = df_1.intersect(df_2)
    val except_df = df_1.except(df_2)

    // DataFrame Miscellaneous Operations
    val df_3 = Seq((1, Array("a", "b", "c"))).toDF("id", "arrayCol")
    val exploded_df = df_3.withColumn("exploded", explode($"arrayCol"))
    val df_4 = Seq((1, "a", 1), (1, "b", 2), (1, "c", 3)).toDF("id", "arrayCol", "valueColumn")
    val pivot_df = df_4.groupBy("id").pivot("arrayCol").agg(sum("valueColumn"))

    // DataFrame Window Functions
    val window_spec = Window.partitionBy("id").orderBy("id")
    val cum_sum_df = df_3.withColumn("cumSum", sum("id").over(window_spec))

    // DataFrame Input/Output
    df_3.write.format("json").save("path/to/json1")

    // Dataset Operations
    val data = Seq(1, 2, 3, 4, 5)
    val dataset = spark.createDataset(data)
    val json_dataset = spark.read.json("path/to/json1")
    val mapped_dataset = dataset.map(_ * 2)
    val flat_mapped_dataset = dataset.flatMap(x => Seq(x, x * 2))
    val filtered_dataset = dataset.filter(_ > 2)
    val distinct_dataset = dataset.distinct()
    val sampled_dataset = dataset.sample(false, 0.5)
    val union_dataset = dataset.union(dataset)
    val joined_dataset = dataset.join(dataset, dataset("value") === dataset("value"))
    val grouped_dataset = dataset.groupBy("value")

    // Dataset Actions
    val collected_data = dataset.collect()
    val count = dataset.count()
    val first_element = dataset.first()
    val first_n_elements = dataset.take(5)
    dataset.show()

    // Dataset Persistence
    dataset.cache()
    dataset.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    // Dataset Partitioning
    val repartitioned_dataset = dataset.repartition(10)
    val coalesced_dataset = dataset.coalesce(1)

    // Dataset File Output
    dataset.write.format("parquet").save("path/to/parquet1")

    // Sample RDD
    val sc = spark.sparkContext
    val rdd_1 = sc.parallelize(Seq((1, "Alice"), (2, "Bob")))
    val rdd_2 = sc.parallelize(Seq((1, "NY"), (2, "LA")))

    // RDD Transformations
    val mapped_rdd = rdd_1.map(x => (x._1, x._2.toUpperCase))
    val flat_mapped_rdd = rdd_1.flatMap(x => List(x, (x._1, x._2.toLowerCase)))
    val filtered_rdd = rdd_1.filter(_._1 > 1)
    val distinct_rdd = rdd_1.distinct()
    val sampled_rdd = rdd_1.sample(false, 0.5)
    val union_rdd = rdd_1.union(rdd_2)
    val intersection_rdd = rdd_1.intersection(rdd_2)
    val subtracted_rdd = rdd_1.subtract(rdd_2)
    val cartesian_rdd = rdd_1.cartesian(rdd_2)

    // RDD Actions
    val collected_rdd = rdd_1.collect()
    val count_rdd = rdd_1.count()
    val first_rdd = rdd_1.first()
    val take_rdd = rdd_1.take(5)
    val reduced_rdd = rdd_1.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val folded_rdd = rdd_1.fold((0, ""))((acc, x) => (acc._1 + x._1, acc._2 + x._2))
    val aggregated_rdd = rdd_1.aggregate((0, ""))((acc, x) => (acc._1 + x._1, acc._2 + x._2), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    rdd_1.foreach(println)

    // RDD Persistence
    rdd_1.cache()
    rdd_1.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    // RDD Partitioning
    val repartitioned_rdd = rdd_1.repartition(10)
    val coalesced_rdd = rdd_1.coalesce(1)

    // Key-Value Pair RDD Operations
    val pair_rdd = rdd_1.map(x => (x._1, x._2))
    val reduced_by_key_rdd = pair_rdd.reduceByKey(_ + _)
    val grouped_by_key_rdd = pair_rdd.groupByKey()
    val joined_pair_rdd = pair_rdd.join(pair_rdd)
    val left_outer_joined_pair_rdd = pair_rdd.leftOuterJoin(pair_rdd)
    val right_outer_joined_pair_rdd = pair_rdd.rightOuterJoin(pair_rdd)
    val cogrouped_rdd = pair_rdd.cogroup(pair_rdd)

    // Additional RDD Operations
    val subtracted_rdd2 = rdd_1.subtract(rdd_2)
    val checkpointed_rdd = {
      sc.setCheckpointDir("path/to/checkpointDir")
      rdd_1.checkpoint()
    }

    // RDD File Output
    rdd_1.saveAsTextFile("path/to/textfile1")
  }
}