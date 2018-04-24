package com.track.guides.sqldemo.examples

import org.apache.spark.sql.SparkSession

object SparkSQLExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSQLExample")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.json("./src/main/scala/com/track/guides/sqldemo/data/people.json")

    df.show()

    spark.stop()


  }

}
