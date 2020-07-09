package com.theprogrammersbook.spark.dataframe

import org.apache.spark.sql.SparkSession

object BroadCastVariable {
  def main(args : Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Spark")
      .master("local")
      .getOrCreate()

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )

    val columns = Seq("firstname","lastname","country","state")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)
    df.show()
    val df2 = df.map(row=>{
      val country = row.getString(2)
      val state = row.getString(3)
      println(broadcastCountries.value)
      println(broadcastCountries.value.get(country))
      println(broadcastCountries.value.get(country).get)
      val fullCountry = broadcastCountries.value.get(country)
      val fullState = broadcastStates.value.get(state)
      (row.getString(0),row.getString(1),fullCountry,fullState)
    }).toDF(columns:_*)

    df2.show(false)
  }
}
