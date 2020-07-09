package com.theprogrammersbook.spark.rdd

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RDDtoDataFrame extends App {
       val spark = SparkSession.builder().appName("Spark").master("local[2]").getOrCreate()
  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd = spark.sparkContext.parallelize(data)

  //From RDD (USING createDataFrame and Adding schema using StructType)
  val schema = StructType(columns
    .map(fieldName => StructField(fieldName, StringType, nullable = true)))
  //convert RDD[T] to RDD[Row]
  val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
  val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)


}
