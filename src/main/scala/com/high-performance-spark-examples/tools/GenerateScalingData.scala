package com.highperformancespark.examples.tools

import com.highperformancespark.examples.dataframe.RawPanda

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.linalg.Vector

object GenerateScalingData {
  /**
   * Generate a Goldilocks data set. We expect the zip code to follow an exponential
   * distribution and the data its self to be normal
   *
   * Note: May generate less than number of requested rows due to different
   * distribution between
   *
   * partitions and zip being computed per partition.
   * @param rows number of rows in the RDD (approximate)
   * @param size number of value elements
   */
  def generateFullGoldilocks(sc: SparkContext, rows: Long, numCols: Int): RDD[RawPanda] = {
    /**
      * result :RDD[scala.Double]
      */
    val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = rows).map(_.toInt.toString) // 100x1的数据
    println(">>>>>>>>>>>>>>>zipRDD")
    println(zipRDD.partitions.size)
//    zipRDD.collect().foreach(println)
    val valuesRDD = RandomRDDs.normalVectorRDD(sc, numRows = rows, numCols = numCols).repartition(zipRDD.partitions.size) // 100x50列
    println(">>>>>>>>>>>>>>>valuesRDD")
    println(valuesRDD.partitions.size)
//    valuesRDD.take(1).foreach(x=>println(x.size))
    val keyRDD = sc.parallelize(1L.to(rows), zipRDD.getNumPartitions)
//    println((keyRDD.partitions.size))
    // 备注：这三个RDD的partition需要相同
    val iteratorResult = keyRDD.zipPartitions(zipRDD, valuesRDD) { // 按照分区zip作。
      (i1, i2, i3) => // 这三个参数分别对应keyRDD、zipRDD、valuesRDD
        new Iterator[(Long, String, Vector)] {
          def hasNext: Boolean = (i1.hasNext, i2.hasNext, i3.hasNext) match {
            case (true, true, true) => true
            case (false, false, false) => false
            // Note: this is "unsafe" (we throw away data when one of
            // the partitions has run out).
            case _ => false
          }
          def next(): (Long, String, Vector) = (i1.next(), i2.next(), i3.next())
        }
    }
    // 得到结果：(1,44,[-1.500796830007573,-1.8545948597548683,0.681917777582775,-0.14342980402585373,-1.490510216972462,-2.955059251167797,-1.2162965394665899,-1.0165040261397624,-1.2173319448017146,1.773588915583311,0.9844745611480102,-0.7329197275494779,-0.688351072750127,0.9539879124279537,-1.281331620138289,0.24864445055045253,0.033356272601377725,-0.4107321648619625,0.8441018280682528,1.420824637846839,-0.18538381070295049,-0.5179957582871524,-0.9147779141835619,-0.5968313628017192,1.6613788632563975,0.9620379942400658,0.14246224526542745,-1.2460405366462157,0.8955414321250725,-0.9339735026838427,-0.4557800378762928,-1.6957691610601993,1.280472985920384,-0.9079874257168407,1.7892413563173337,0.7988649401276974,-0.09298536471278693,0.3243994990159878,-0.11268304311204254,-3.1112130885893428,1.3708146297390231,0.6068245996031407,-0.8182542183615563,-0.3691398695345523,-1.9252072134671832,1.5649632433096015,1.5022710876459626,-0.2850923142254947,-1.124137362028454,0.34911524075999095])
    iteratorResult.foreach(println)

    iteratorResult.map{case (k, z, v) => // 1,44,[]
      RawPanda(k, z, "giant", v(0) > 0.5, v.toArray)
    }

  }

  /**
   * Transform it down to just the data used for the benchmark
   */
  def generateMiniScale(sc: SparkContext, rows: Long, numCols: Int):
      RDD[(Int, Double)] = {
    generateFullGoldilocks(sc, rows, numCols)
      .map(p => (p.zip.toInt, p.attributes(0)))
  }

  /**
   * Transform it down to just the data used for the benchmark
   */
  def generateMiniScaleRows(sc: SparkContext, rows: Long, numCols: Int):
      RDD[Row] = {
    generateMiniScale(sc, rows, numCols).map{case (zip, fuzzy) => Row(zip, fuzzy)}
  }

  // tag::MAGIC_PANDA[]
  /**
   * Generate a Goldilocks data set all with the same id.
   * We expect the zip code to follow an exponential
   * distribution and the data its self to be normal.
   * Simplified to avoid a 3-way zip.
   *
   * Note: May generate less than number of requested rows due to
   * different distribution between partitions and zip being computed
   * per partition.
   */
  def generateGoldilocks(sc: SparkContext, rows: Long, numCols: Int):
      RDD[RawPanda] = {
    val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000,  size = rows)
      .map(_.toInt.toString)
    val valuesRDD = RandomRDDs.normalVectorRDD(
      sc, numRows = rows, numCols = numCols)
    zipRDD.zip(valuesRDD).map{case (z, v) =>
      RawPanda(1, z, "giant", v(0) > 0.5, v.toArray)
    }
  }
  // end::MAGIC_PANDA[]
}
