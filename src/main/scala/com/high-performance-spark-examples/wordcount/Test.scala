package com.highperformancespark.examples.wordcount

import java.io.{File, FileWriter, IOException, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.mapred.lib.aggregate.LongValueMax
import org.apache.spark.AccumulatorParam
import org.apache.spark.sql.SparkSession


object Test {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[2]").getOrCreate()
    val sc  = ss.sparkContext


//    object StringAccumulatorParam extends AccumulatorParam[String] {
//      def zero(initialValue: String): String = {""}
//      def addInPlace(s1: String, s2: String): String = {
//        s1+s2
//      }
//    }
//    val stringAccum = sc.accumulator("")(StringAccumulatorParam)
//    val rdd = sc.parallelize("foo" :: "bar" :: Nil, 2)
//    rdd.foreach(s => stringAccum += s)
//    println(stringAccum.value)

    object MinLongAccumulatorParam extends AccumulatorParam[Long] { // AccumulatorV2
      override def zero(initValue: Long) = initValue
      override def addInPlace(r1: Long, r2: Long): Long = {
        Math.min(r1, r2)
      }
    }
    val acc = sc.accumulator(Long.MaxValue)(MinLongAccumulatorParam) //
    val data = sc.parallelize(List(1L, 2L, 3L) , 2)
    data.foreach(x => acc+= x)
    println(acc.value)

    //    val aparkSession = SparkSession.builder().master("local[2]").getOrCreate()

//    val buf = new StringBuilder
//    val dataString =   "{\"data\":[{\"k\":\"msg_delay_s\",\"v\":" + (5-3)+ "},{\"k\":\"" +  "topicName"  + "_msg_delay_offsets\",\"v\":" + (1000-500)+"}],\"name\":\"segment\",\"namespace\":\"zampda3.smartpixel\",\"time\":"+getNowDate+"}"
//    println(dataString)
//    val resutl = new StringBuilder
//    for (i <- 1 to 2) {
//      resutl ++=
    // ",{\"k\":\"" +"topicNameAndPartition_" + i + "_msg_delay_offsets\",\"v\":" + (1000-300) +"}"
//    }
//
//
//    val dataString1 =   "{\"data\":[{\"k\":\"msg_delay_s\",\"v\":" + (5-3)+ "}" + resutl.toString() +"],\"name\":\"segment\",\"namespace\":\"zampda3.smartpixel\",\"time\":"+getNowDate+"}"
//    println(dataString1)
//
//
//    val studentInfoMutable=scala.collection.mutable.Map("john" -> 21, "stephen" -> 22,"lucy" -> 20)
//    println(40 - studentInfoMutable.getOrElse("john3", 20))

    //    println(getNowDate)
  }

  private def getNowDate: String = {
    val now: Date = new Date()
//    println(now.getTime / 1000)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(now)

  }


}
