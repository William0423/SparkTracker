package com.highperformancespark.examples.wordcount

/**
 * What sort of big data book would this be if we didn't mention wordcount?
 */
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object WordCount {
  // bad idea: uses group by key
  def badIdea(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val grouped = wordPairs.groupByKey()
    val wordCounts = grouped.mapValues(_.sum)
    wordCounts
  }

  // good idea: doesn't use group by key
  //tag::simpleWordCount[]
  def simpleWordCount(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }
  //end::simpleWordCount[]

  /**
    * Come up with word counts but filter out the illegal tokens and stop words
    */
  //tag::wordCountStopwords[]
  def withStopWordsFiltered(rdd : RDD[String], illegalTokens : Array[Char],
    stopWords : Set[String]): RDD[(String, Int)] = {

    println(illegalTokens)


    val separators = illegalTokens ++ Array[Char](' ')

    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(separators)

    val tokens: RDD[String] = rdd.flatMap(_.split(separators).
      map(_.trim.toLowerCase))
    println(">>>>>><<<<<<<<<<<<<<")
    tokens.collect().foreach(println)
    val words = tokens.filter(token =>
      !stopWords.contains(token) && (token.length > 0) )
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }
  //end::wordCountStopwords[]



  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext

    val wordRDD = sc.parallelize(Seq(
      "How happy was the panda? You ask.",
      "Panda is the most happy panda in all the #$!?ing land!"))

    val stopWords: Set[String] = Set("a", "the", "in", "was", "there", "she", "he")
    val illegalTokens: Array[Char] = "#$%?!.".toCharArray

    val wordCounts = WordCount.withStopWordsFiltered(
      wordRDD, illegalTokens, stopWords)
    val wordCountsAsMap = wordCounts.collectAsMap()
    assert(!wordCountsAsMap.contains("the"))
    assert(!wordCountsAsMap.contains("?"))
    assert(!wordCountsAsMap.contains("#$!?ing"))
    assert(wordCountsAsMap.contains("ing"))
    assert(wordCountsAsMap.get("panda").get.equals(3))
  }


}
