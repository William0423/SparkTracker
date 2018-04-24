package com.track.guides.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TransformationOpera {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Transformation operations basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext

//     rePartitions(sc)

    // splitRDD(sc)
    // unionRDD(sc)
    // mapOperationByPartitions(sc)
    // zipRDD(sc)
    // zipRddById(sc)
    // rddCombineByKey(sc)
    // rddGroupByKey(sc)

    // rddCogroup(sc)

    rddFoldByKey(sc)
  }

  private def rddCogroup(sc: SparkContext): Unit = {

    val rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),4)
    val rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
    val rdd3 = sc.makeRDD(Array(("A","A"),("E","E")),2)

    val rdd4 = rdd1.cogroup(rdd2,rdd3)
    println(rdd4.partitions.size)
    rdd4.collect().foreach(println)

  }

  private def rddGroupByKey(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    rdd1.groupByKey().collect().foreach(println)


  }

  /**
    * http://lxw1234.com/archives/2015/07/358.htm
    * @param sc
    */
  private def rddFoldByKey(sc: SparkContext): Unit = {
    val l = List(1,2,3,4)
    val result = l.fold(1)((x, y) => x + y)
    println(">>>>>>")
    println(result)

  }

  private def rddCombineByKey(sc:SparkContext): Unit = {
    val rdd1 = sc.makeRDD( Array(("A",1),("A",2),("B",1),("B",2),("C",1), ("A",3)) )
    println("#################")
    rdd1.combineByKey(
      // 备注：此处对 value的值进行了类型限制为Int的，如果是不同类型，可以取消这个限制
      (v : Int) => v + "_", // 返回的结果是String类型的
      (c : String, v : Int) => c + "@" + v,
      (c1 : String, c2 : String) => c1 + "$" + c2).collect().foreach(println)

    val rdd2 = sc.makeRDD(Array(("hello", "world"),
      ("hello", "ketty"),
      ("hello", "Tom"),
      ("Sam", "love"),
      ("Sam", "sorry"),
      ("Tom", "big"),
      ("Tom", "shy")
    ))

    rdd2.combineByKey(
      (v) => (1,v), // (“hello”, (1, “world”))
      (acc:(Int, String), v) => (acc._1+1,acc._2), // ((“hello”, (1, “world”))、(“hello”, “ketty”)
      (acc1:(Int, String), acc2:(Int, String)) => (acc1._1+acc2._1, acc2._2) // (“hello”, (2, “ketty”))、(“hello”, (1, “Tom”))
    ).sortBy(_._2, false).map{
      case(key, (key1, value)) => Array(key, key1.toString, value).mkString("\t")
    }.collect().foreach(println)


  }


  private def zipRddById(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Seq("A","B","C","D","E","F"),3)
    rdd1.zipWithIndex().collect().foreach(println)
    // 初始化，并通过分区数进行累加。
    rdd1.zipWithUniqueId().collect.foreach(println)
  }



  /**
    * 重新分区算子
    * @param sc
    */
  private def rePartitions(sc:SparkContext): Unit = {

    val data = sc.textFile("./src/main/scala/com/sch/data/person.txt").map(_.split(","))

    data.collect().foreach(println)
    println(data.partitions.size)

    val rdd1 = data.coalesce(4)
    println(rdd1.partitions.size)

    val rdd2 = data.coalesce(4, true)
    println(rdd2.partitions.size)
  }


  /**
    * 拆分rdd
    * @param sc
    */
  private def splitRDD(sc: SparkContext): Unit = {
    // 每个rdd有10个分区，
    val rdd = sc.makeRDD(1 to 10,10)
    println("<<<<<")
    println(rdd.partitions.size)

    println(">>>>>>>")
    val spliteRDD = rdd.randomSplit(Array(1.0,2.0,3.0,4.0)) // 返回的是一个数组rdd
    println(spliteRDD.size)
    // 切分后的rdd还是有10个分区
    println(spliteRDD(0).partitions.size)
    println(spliteRDD(1).partitions.size)

    println("@@@@@@@@@@")
    spliteRDD.foreach(a=>a.collect().foreach(println))

    println("========================================")
    val rddGlom = sc.makeRDD(1 to 10, 3)

    val rdd1 = rddGlom.glom()
    // http://lxw1234.com/archives/2015/07/343.htm
    println(rdd1.partitions.size)
  }

  /**
    * 合并和去重：http://lxw1234.com/archives/2015/07/345.htm
    * @param sc
    */
  private def unionRDD(sc:SparkContext): Unit = {
    val rdd1 = sc.makeRDD(1 to 2,1)
    rdd1.collect().foreach(println)
    val rdd2 = sc.makeRDD(2 to 3,1)
    rdd2.collect().foreach(println)
    val rdd3 = rdd1.union(rdd2)
    rdd3.collect().foreach(println)
  }

  /**
    * 根据分区进行map操作
    * @param sc
    */
  private def mapOperationByPartitions(sc: SparkContext): Unit = {
    val rdd1= sc.makeRDD(1 to 5, 2)

    val rdd3 = rdd1.mapPartitions{x => {
      val result = List[Int]()
      var i = 0
      var temp = 0
      while (x.hasNext) {
        temp = x.next()
        println(temp)
        i += temp
      }
      println(">>>>>>>>>>>>")
      result.::(i).iterator
    }
    }
    rdd3.collect().foreach(println)


    // 按照分区的索引对Int型元素进行汇总累加：
    val rdd2 = rdd1.mapPartitionsWithIndex((x,iter) => {
      val result = List[String]()
      var i = 0
      while(iter.hasNext) {
        i += iter.next()
      }
      result.::(x +"|" +i).iterator
    })
    rdd2.collect().foreach(println)

    println("@@@@@@@@@@@@@@@@@@@@@@@@")

    // 统计每个分区的具体元素
    rdd1.mapPartitionsWithIndex((ind,iter) =>{
      val partiationMap = scala.collection.mutable.Map[String, List[Int]]()
      while (iter.hasNext) {
        val pName = "partition_" + ind
        val elem = iter.next()

        if (partiationMap.contains(pName)) {
          var eleList = partiationMap(pName)
          eleList ::= elem
          partiationMap(pName) = eleList
        } else {
          partiationMap(pName) = List[Int]{elem}
        }

      }

      partiationMap.iterator
    }).collect().foreach(println)


  }

  /**
    * zip的要求：rdd之间的分区相同，每个分区里面的元素个数相同
    * http://lxw1234.com/archives/2015/07/350.htm
    * @param sc
    */
  private def zipRDD(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(1 to 10,2)
    val rdd2 = sc.makeRDD(Seq("A","B","C","D","E"),2)
    // 按照分区，需要判断是否还存在元素
    rdd1.zipPartitions(rdd2) {
      (rdd1Iter, rdd2Iter) => {
        var result = List[String]()
        while(rdd1Iter.hasNext && rdd2Iter.hasNext) {
          result::=(rdd1Iter.next() +"_" + rdd2Iter.next())
        }
        result.iterator
      }
    }.collect().foreach(println)
    // 三个rdd做zip
  }


}
