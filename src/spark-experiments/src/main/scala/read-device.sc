import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}

val spark = SparkSession.builder()
   .appName("parquet.net")
   .master("local[1]")
   .getOrCreate()

import spark.implicits._
val sc = spark.sparkContext

val devices = spark.read.parquet("C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\map.parquet");

devices.printSchema()

devices.show()

val i = 0