package com.track.guides.rdd

import org.apache.spark.sql.SparkSession

object ActionOpera {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Transformation operations basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext



  }

}
