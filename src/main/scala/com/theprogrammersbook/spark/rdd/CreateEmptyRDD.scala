package com.theprogrammersbook.spark.rdd

import java.util.Date

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD extends App{
 // Creating sparkSession with master as 3
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("CreateEmptyRDD")
    .getOrCreate()
  // Creating emptyRDD with sparkContext
  val rdd = spark.sparkContext.emptyRDD
  // Creating emptyRDD of String with sparkContext
  val rddString = spark.sparkContext.emptyRDD[String]

  println(rdd)
  println(rddString)
  println("Num of Partitions: "+rdd.getNumPartitions)

  rddString.saveAsTextFile("test.txt"+new Date().getTime)
 //Operation will be done but with no data ... because we do not have any data

  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Num of Partitions of Seq empty string: "+rdd2.getNumPartitions)

  rdd2.saveAsTextFile("test3.txt"+new Date().getTime)
  // Pair RDD

  type dataType = (String,Int)
  var pairRDD = spark.sparkContext.emptyRDD[dataType]
  println(pairRDD)


}
