package com.theprogrammersbook.spark.rdd

import org.apache.spark.sql.SparkSession

object AccumulatorExample extends App {
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  val longAdd = spark.sparkContext.longAccumulator("AddAccumulator")

  val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

  rdd.foreach(x => longAdd.add(x))
  // we will example some of the methods
  println(longAdd.value)
  println(longAdd.sum)
  println(longAdd.avg)
  println(longAdd.isZero)
 var copyLongAcc =  longAdd.copy()
  println(copyLongAcc.value)
  println(longAdd.count)
  //longAdd.reset()
  //println(longAdd.value)
  longAdd.merge(copyLongAcc)
  println(longAdd.value)
  println("Collection Accumulator")
  val collectionAcc = spark.sparkContext.collectionAccumulator[List[String]]("CollectionAccumulator")
  val rdd2 = spark.sparkContext.parallelize(Array("Good", "Best", "Luck"))

  rdd2.foreach(x => collectionAcc.add(List(x)))
  println(collectionAcc.value)


}
