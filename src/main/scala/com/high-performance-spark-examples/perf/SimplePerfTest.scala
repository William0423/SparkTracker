/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.highperformancespark.examples.perf

import com.highperformancespark.examples.dataframe.RawPanda
import com.highperformancespark.examples.tools._

import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.types._

/**
 * A simple performance test to compare a simple sort between DataFrame, and RDD
  * 比较rdd和dataframe的排序：
  * dataFrame不如RDD？
 */
object SimplePerfTest {

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("simple-perf-test")

//    val sparkSession = SparkSession.builder().master("local[2]").enableHiveSupport().getOrCreate()
    val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext

    /**
      * 100
      */
    val scalingFactor = if (args.length > 0) args(0).toLong else 100L
    println(">>>>>>>>>> scalingFactor")
    println(scalingFactor)
    /**
      * 50
      */
    val size = if (args.length > 1) args(1).toInt else 50
    run(sc, sparkSession, scalingFactor, size)

  }

  def run(sc: SparkContext, session: SparkSession, scalingFactor: Long, size: Int) = {
    import session.implicits._

    /**
      * 是一个RDD[RawPanda]类型的数据
      * case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean, attributes: Array[Double])
      * 生成100个RawPanda(1,2691,giant,true,[D@617389a)
      */
    val inputRDD = GenerateScalingData.generateFullGoldilocks(sc, scalingFactor, size)

    println(">>>>>>>>>>>>>>    start  ")
    val accm = sc.longAccumulator("count")

    println(">>>>>>>>>>>>>>    accm  ")
    println(accm.value)
//    println(">>>>>>>>>>>>>>>>>>>>  inputRDD")
//    inputRDD.collect().foreach(x=>accm.add(1))
    inputRDD.map(x => accm.add(1))
    println(accm.value)

    // case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean, attributes: Array[Double])
    // demo: RawPanda(1,2691,giant,true,[D@617389a)
    // RDD[(Int, Double)]
    val pairRDD:RDD[(Int, Double)] = inputRDD.map(p => (p.zip.toInt, p.attributes(0)))
    pairRDD.cache()
    // 调用count 会导致第一次计算 RDD。
    pairRDD.count() // 100
    println(">>>>>>>>>>>>>>>>>  pairRDD")
    pairRDD.collect().foreach(println)
    println(pairRDD.collect().size)

    val tempBd = sc.broadcast(1) // 做10次map
    val rddTimeings = 1.to(tempBd.value).map(x => time(testOnRDD(pairRDD)))
    println(">>>>>>>>>>>>>>>>>>>>>rddTimeings count")
    // Vector((100,126813780))
    println(rddTimeings)
    // 做10次group
    val groupTimeings = 1.to(tempBd.value).map(x => time(groupOnRDD(pairRDD)))
    println(">>>>>>>>>>>>>>>>>>>>>groupTimeings count")
    println(groupTimeings)

    // rdd to dataframe
    val df = inputRDD.toDF()
    val inputDataFrame = df.select(df("zip").cast(IntegerType), df("attributes")(0).as("fuzzyness").cast(DoubleType))
    inputDataFrame.cache()
    inputDataFrame.count()
    /**
      * root
          |-- zip: integer (nullable = true)
          |-- fuzzyness: double (nullable = true)
      */
    inputDataFrame.printSchema()
    /**
      * +----+--------------------+
      * | zip|           fuzzyness|
      * +----+--------------------+
      * |   2|-0.02558527485766...|
      * | 792|-0.43405940233501517|
      */
    inputDataFrame.show()

    val dataFrameTimeings = 1.to(tempBd.value).map(x => time(testOnDataFrame(inputDataFrame)))
    println(">>>>>>>>>>>>>>>>>>>>>dataFrameTimeings count")
    println(dataFrameTimeings)

    /**
      * 213878398,104531981,79756548,109261023,111954594,88327038,68668609,69592331,72732983,64293045
      * 411689383,119911323,104759422,113625914,106296494,97318745,85786600,89377207,82206667,91472617
      * 2498120536,1236456195,3084303309,1070865014,843600460,891456606,726206174,4129305772,1238943263,1984751621
      */
    println(rddTimeings.map(_._2).mkString(","))
    println(groupTimeings.map(_._2).mkString(","))
    println(dataFrameTimeings.map(_._2).mkString(","))

  }

  /**
    * 一次reduceByKey
    * @param rdd
    * @return
    */
  def testOnRDD(rdd: RDD[(Int, Double)]) = {
    val result = rdd.map{case (x, y) => (x, (y, 1))}.reduceByKey{case (x, y) => (x._1 + y._1, x._2 + y._2)}
    println(">>>>>>>>>>>>>>>>> after reduceBy")
    result.collect().foreach(println)
    result.count()
  }


  def groupOnRDD(rdd: RDD[(Int, Double)]) = {
    val result = rdd.groupByKey().mapValues{v => v.aggregate((0.0, 0))(
      {case (x, y) => (x._1 + y, x._2 + 1)},
      {case (x, y) => (x._1 + y._1, x._2 + y._2)})}
    println(">>>>>>>>>>>>>>>>> after groupByKey")
    result.collect().foreach(println)
    result.count()
  }

  def testOnDataFrame(df: DataFrame) = {
    val resultDf = df.groupBy("zip").avg("fuzzyness")
    println(">>>>>>>>>>>>>>>>> after groupByKey")
    resultDf.printSchema()
    resultDf.show()

    resultDf.count()
  }

  def time[R](block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Time ${t1 - t0}ns")
    (result, t1 - t0)
  }

}
