package com.highperformancespark.examples.tokenize

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SampleTokenize {
  //tag::DIFFICULT[]
  def difficultTokenizeRDD(input: RDD[String]) = {
    input.flatMap(_.split(" "))
  }
  //end::DIFFICULT[]

  //tag::EASY[]
  def tokenizeRDD(input: RDD[String]) = {
    input.flatMap(tokenize)
  }

  protected[tokenize] def tokenize(input: String) = {
    input.split(" ")
  }
  //end::EASY[]

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext

    val input = List("hi holden", "I like coffee")
    val expected = List("hi", "holden", "I", "like", "coffee")

    val inputRDD = sc.parallelize(input)
    val result = SampleTokenize.difficultTokenizeRDD(inputRDD).collect()

    if (result.toList == expected) {
      System.out.println("Yes")
    } else {
      System.out.println("No")
    }


    /**
      * Two
      */
    val inputRDD1 = sc.parallelize(input)
    val result1 = SampleTokenize.tokenizeRDD(inputRDD1).collect()

    if (result1.toList == expected) {
      System.out.println("Yes")
    } else {
      System.out.println("No")
    }


    /**
      * three
      */

    val resutl2 = SampleTokenize.tokenize("hi holden").toList == List("hi", "holden")
    if (resutl2)   {
      System.out.println("Yes")
    } else {
      System.out.println("No")
    }


  }
}
