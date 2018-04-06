package com.track.streaming

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object ConcurrentJobsDemo {

  def main(args: Array[String]) {

    // 完整代码可见本文最后的附录
    val BLOCK_INTERVAL = 1 // in seconds
    val BATCH_INTERVAL = 5 // in seconds
    val CURRENT_JOBS = 10

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getSimpleName)
    conf.setMaster("local[2]")
    conf.set("spark.streaming.blockInterval", s"${BLOCK_INTERVAL}s")
    conf.set("spark.streaming.concurrentJobs", s"${CURRENT_JOBS}")
    val ssc = new StreamingContext(conf, Seconds(BATCH_INTERVAL))

    // DStream DAG 定义开始
    val inputStream = ssc.receiverStream(new MyReceiver)
    inputStream.foreachRDD(_ => Thread.sleep(Int.MaxValue)) // output 1
    inputStream.foreachRDD(_ => Thread.sleep(Int.MaxValue)) // output 2
    // DStream DAG 定义结束

    ssc.start()
    ssc.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    override def onStart() {
      // invoke store("str") every 100ms
      Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable {
        override def run(): Unit = store("str")
      }, 0, 100, TimeUnit.MILLISECONDS)
    }

    override def onStop() {}
  }

}
