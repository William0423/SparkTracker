package com.highperformancespark.examples.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.MutableList
import scala.collection.{Map, mutable}

object GoldilocksGroupByKey {
  //tag::groupByKey[]
  def findRankStatistics(dataFrame: DataFrame, ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    //Map to column index, value pairs
    val pairRDD: RDD[(Int, Double)] = mapToKeyValuePairs(dataFrame)

    println("############ pairRDD.collect().foreach(println)############")
    pairRDD.collect().foreach(println)

    // 相同key的进行group:
    // (4,CompactBuffer(9.0, 8.0, 1.0, 7.0, 2.0, 0.0, 6.0, 3.0, 4.0, 5.0))
    //(0,CompactBuffer(8.0, 3.0, 5.0, 0.0, 4.0, 2.0, 7.0, 1.0, 9.0, 6.0))
    //(2,CompactBuffer(9.0, 5.0, 3.0, 7.0, 6.0, 8.0, 1.0, 4.0, 2.0, 0.0))
    //(1,CompactBuffer(1.0, 5.0, 9.0, 3.0, 0.0, 8.0, 4.0, 2.0, 6.0, 7.0))
    //(3,CompactBuffer(2.0, 9.0, 6.0, 3.0, 8.0, 1.0, 4.0, 7.0, 5.0, 0.0))
    val groupColumns: RDD[(Int, Iterable[Double])] = pairRDD.groupByKey()
    println("############ groupColumns ############")
    groupColumns.collect().foreach(println)


    groupColumns.mapValues(
      iter => {
        //convert to an array and sort
        val sortedIter = iter.toArray.sorted
        println(">>>>>>>>>>>>>sorteIter>>>>>>>>>>>>>>>>>")

        sortedIter.toIterable.zipWithIndex.flatMap({
        case (colValue, index) =>
            if (ranks.contains(index + 1)) {
              Iterator(colValue)
            } else {
              Iterator.empty
            }
      })
    }).collectAsMap()

  }

  def findRankStatistics(
    pairRDD: RDD[(Int, Double)],
    ranks: List[Long]): Map[Int, Iterable[Double]] = {
    assert(ranks.forall(_ > 0))
    pairRDD.groupByKey().mapValues(iter => {
      val sortedIter  = iter.toArray.sorted
      sortedIter.zipWithIndex.flatMap(
        {
        case (colValue, index) =>
            if (ranks.contains(index + 1)) {
              //this is one of the desired rank statistics
              Iterator(colValue)
            } else {
              Iterator.empty
            }
        }
      ).toIterable //convert to more generic iterable type to match out spec
    }).collectAsMap()
  }
  //end::groupByKey[]


  //tag::toKeyValPairs[]
  def mapToKeyValuePairs(dataFrame: DataFrame): RDD[(Int, Double)] = {
    val rowLength = dataFrame.schema.length
    dataFrame.rdd.flatMap(
      // 对于dataframe的每个row，从第0列到第rowLength列，取出每列的值：即先按行遍历，再列遍历
      // 比如结果：(0,2.0) (1,7.0) (2,1.0) (3,3.0) (4,2.0) ————第一行的5列数据
      row => Range(0, rowLength).map(i => (i, row.getDouble(i)))
    )
  }
  //end::toKeyValPairs[]


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val testRanks = List(3L, 8L) // 转化到数组里面，要找没一列的第2个数和第7个数
    val (smallTestData, result) = DataCreationUtils.createLocalTestData(5, 10, testRanks)
    val schema = StructType(
      result.keys.toSeq.map(
        n => StructField("Column" + n.toString, DoubleType)
      ))
    val smallTestDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(smallTestData), schema)

    println(">>>>>>>testRanks>>>>>>>>")
    println(testRanks)

    println("#############  result ##################")
    println(result)

    println(smallTestDF.schema.length)
    smallTestDF.printSchema()
    smallTestDF.show()

    testGoldilocksImplementations(smallTestDF, testRanks, result)
  }


  /**
    * 要求：找到排序后的dataframe中每列元素的第k个值，
    * @param data
    * @param targetRanks
    * @param expectedResult
    */
  def testGoldilocksImplementations(data: DataFrame, targetRanks: List[Long], expectedResult: Map[Int, Iterable[Long]]) = {

    // 得到的结果：
    // Map(0 -> WrappedArray(2.0, 7.0), 1 -> WrappedArray(2.0, 7.0), 2 -> WrappedArray(2.0, 7.0), 3 -> WrappedArray(2.0, 7.0), 4 -> WrappedArray(2.0, 7.0))
    val iterative = GoldilocksWhileLoop.findRankStatistics(data, targetRanks)
    println("===========val iterative = GoldilocksWhileLoop.findRankStatistics(data, targetRanks)=============")
    println(iterative)
    // 得到的结果【不是排好序的】：
    // Map(2 -> ArrayBuffer(2.0, 7.0), 4 -> ArrayBuffer(2.0, 7.0), 1 -> ArrayBuffer(2.0, 7.0), 3 -> ArrayBuffer(2.0, 7.0), 0 -> ArrayBuffer(2.0, 7.0))
    val groupByKey = GoldilocksGroupByKey.findRankStatistics(data, targetRanks)

    println("########## val groupByKey = GoldilocksGroupByKey.findRankStatistics(data, targetRanks) ###########")
    println(groupByKey)

    // Map(2 -> CompactBuffer(2.0, 7.0), 4 -> CompactBuffer(2.0, 7.0), 1 -> CompactBuffer(2.0, 7.0), 3 -> CompactBuffer(2.0, 7.0), 0 -> CompactBuffer(2.0, 7.0))
    val firstTry = GoldilocksFirstTry.findRankStatistics(data, targetRanks)
    println("########## val firstTry = GoldilocksFirstTry.findRankStatistics(data, targetRanks) ###########")
    println(firstTry)

    // Map(2 -> CompactBuffer(2.0, 7.0), 4 -> CompactBuffer(2.0, 7.0), 1 -> CompactBuffer(2.0, 7.0), 3 -> CompactBuffer(2.0, 7.0), 0 -> CompactBuffer(2.0, 7.0))
    val hashMap = GoldilocksWithHashMap.findRankStatistics(data, targetRanks)
    println("############## hashMap ##############")
    println(hashMap)


    // 排好序的
    val secondarySort = GoldilocksSecondarySort.findRankStatistics(data, targetRanks, data.rdd.partitions.length)
    println("############## val secondarySort = GoldilocksSecondarySort.findRankStatistics(data, targetRanks, data.rdd.partitions.length) ##############")
    println(secondarySort)

    // 排好序的
    val secondarySortV2 = GoldilocksSecondarySortV2.findRankStatistics(data, targetRanks)
    println("##################  secondarySortV2  ######################")
    println(secondarySortV2)


    expectedResult.foreach {
      case((i, ranks)) =>
        assert(iterative(i).equals(ranks),
          "The Iterative solution to goldilocks was incorrect for column " + i)
        assert(groupByKey(i).equals(ranks),
          "Group by key solution was incorrect")
        assert(firstTry(i).equals(ranks),
          "GoldilocksFirstTry incorrect for column " + i )
        assert(hashMap(i).equals(ranks),
          "GoldilocksWithhashMap incorrect for column " + i)

//        assert(secondarySort(i).equals(ranks))
//
//
//        assert(secondarySortV2(i).equals(ranks))

    }





  }



}


object DataCreationUtils {
  def createLocalTestData(numberCols: Int, numberOfRows: Int,
                          targetRanks: List[Long]) = {

    val cols = Range(0, numberCols).toArray
    val scalers = cols.map(x => 1.0)
    val rowRange = Range(0, numberOfRows)
    val columnArray: Array[IndexedSeq[Double]] = cols.map(
      columnIndex => {
        val columnValues = rowRange.map(
          x => (Math.random(), x)).sortBy(_._1).map(_._2 * scalers(columnIndex))
        columnValues
      })
    val rows = rowRange.map(
      rowIndex => {
        Row.fromSeq(cols.map(colI => columnArray(colI)(rowIndex)).toSeq)
      })


    val result: Map[Int, Iterable[Long]] = cols.map(i => {
      (i, targetRanks.map(r => Math.round((r - 1) / scalers(i))))
    }).toMap

    (rows, result)
  }

}


object GoldilocksWhileLoop{

  //tag::rankstatsLoop[]
  def findRankStatistics(
    dataFrame: DataFrame,
    ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    // df的所有列
    val numberOfColumns = dataFrame.schema.length
    var i = 0
    var  result = Map[Int, Iterable[Double]]()

    while(i < numberOfColumns){
      // 把df转化为rdd，然后按列提取元素
      val col = dataFrame.rdd.map(row => row.getDouble(i))
      col.collect().foreach(println)
      println("##################")

      // 排序所有列后，使用下标做zip
      val sortedCol : RDD[(Double, Long)] = col.sortBy(v => v).zipWithIndex()
      sortedCol.collect().foreach(println)
      println(">>>>>>>>>>>>>>>>")

      val ranksOnly = sortedCol.filter{
        //rank statistics are indexed from one. e.g. first element is 0
        // 找到下标存在于ranks列表中的list元素
        case (colValue, index) =>  ranks.contains(index + 1)
      } // 此处得到的结果是(2.0,2) (7.0,7) ()
      println(ranksOnly.collect().foreach(println))
      println(ranksOnly.keys.collect().foreach(println))

      println("^^^^^^^^^^^^^^^^^^^")

      val list = ranksOnly.keys.collect() // 所有的key值转化为数组，然后遍历成一个map
      result += (i -> list)
      i+=1
    }
    result
  }
  //end::rankstatsLoop[]
}


object GoldilocksFirstTry {

  /**
    * Find nth target rank for every column.：寻找每一列的第一和第三个元素
    *
    * For example:
    *
    * dataframe:
    *   (0.0, 4.5, 7.7, 5.0)
    *   (1.0, 5.5, 6.7, 6.0)
    *   (2.0, 5.5, 1.5, 7.0)
    *   (3.0, 5.5, 0.5, 7.0)
    *   (4.0, 5.5, 0.5, 8.0)
    *
    * targetRanks:
    *   1, 3
    *
    * The output will be:
    *   0 -> (0.0, 2.0)
    *   1 -> (4.5, 5.5)
    *   2 -> (7.7, 1.5)
    *   3 -> (5.0, 7.0)
    *
    * @param dataFrame dataframe of doubles
    * @param targetRanks the required ranks for every column
    *
    * @return map of (column index, list of target ranks)
    */
  //tag::firstTry[]
  def findRankStatistics(dataFrame: DataFrame, targetRanks: List[Long]):
    Map[Int, Iterable[Double]] = {

    val valueColumnPairs: RDD[(Double, Int)] = getValueColumnPairs(dataFrame)
    val sortedValueColumnPairs = valueColumnPairs.sortByKey()
    sortedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfColumns = dataFrame.schema.length

    // 聚合
    val partitionColumnsFreq = getColumnsFreqPerPartition(sortedValueColumnPairs, numOfColumns)

    //
    val ranksLocations = getRanksLocationsWithinEachPart(targetRanks, partitionColumnsFreq, numOfColumns)

    //
    val targetRanksValues = findTargetRanksIteratively(sortedValueColumnPairs, ranksLocations)

    targetRanksValues.groupByKey().collectAsMap()
  }
  //end::firstTry[]

  /**
   * Step 1. Map the rows to pairs of (value, column Index).
   *
   * For example:
   *
   * dataFrame:
   *     1.5, 1.25, 2.0
   *    5.25,  2.5, 1.5
   *
   * The output RDD will be:
   *    (1.5, 0) (1.25, 1) (2.0, 2) (5.25, 0) (2.5, 1) (1.5, 2)
   *
   * @param dataFrame dateframe of doubles
   *
   * @return RDD of pairs (value, column Index)
   */
  //tag::firstTry_Step1[]
  private def getValueColumnPairs(dataFrame : DataFrame): RDD[(Double, Int)] = {
    dataFrame.rdd.flatMap{
      row: Row => row.toSeq.zipWithIndex
                  .map{
                    case (v, index) => (v.toString.toDouble, index)}
    }
  }
  //end::firstTry_Step1[]

  /**
   * Step 2. Find the number of elements for each column in each partition.
   *
   * For Example:
   *
   * sortedValueColumnPairs:
   *    Partition 1: (1.5, 0) (1.25, 1) (2.0, 2) (5.25, 0)
   *    Partition 2: (7.5, 1) (9.5, 2)
   *
   * numOfColumns: 3
   *
   * The output will be:
   *    [(0, [2, 1, 1]), (1, [0, 1, 1])]
   *
   * @param sortedValueColumnPairs - sorted RDD of (value, column Index) pairs
   * @param numOfColumns the number of columns
   *
   * @return Array that contains
    *         (partition index,
   *           number of elements from every column on this partition)
   */
  //tag::firstTry_Step2[]
  private def getColumnsFreqPerPartition(sortedValueColumnPairs: RDD[(Double, Int)],
    numOfColumns : Int):
    Array[(Int, Array[Long])] = {

    val zero = Array.fill[Long](numOfColumns)(0)

    def aggregateColumnFrequencies (partitionIndex : Int,
      valueColumnPairs : Iterator[(Double, Int)]) = {
      val columnsFreq : Array[Long] = valueColumnPairs.aggregate(zero)(
        (a : Array[Long], v : (Double, Int)) => {
          val (value, colIndex) = v
          //increment the cell in the zero array corresponding to this column index
          a(colIndex) = a(colIndex) + 1L
          a
        },
        (a : Array[Long], b : Array[Long]) => {
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        })

      Iterator((partitionIndex, columnsFreq))
    }

    sortedValueColumnPairs.mapPartitionsWithIndex(
      aggregateColumnFrequencies).collect()
  }
  //end::firstTry_Step2[]

  /**
   * Step 3: For each Partition determine the index of the elements that are
   * desired rank statistics.
   *
   * This is done locally by the driver.
   *
   * For Example:
   *
   *    targetRanks: 5
   *    partitionColumnsFreq: [(0, [2, 3]), (1, [4, 1]), (2, [5, 2])]
   *    numOfColumns: 2
   *
   * The output will be:
   *
   *    [(0, []), (1, [(0, 3)]), (2, [(1, 1)])]
   *
   * @param partitionColumnsFreq Array of
   *                             (partition index,
   *                              columns frequencies per this partition)
   *
   * @return  Array that contains
   *         (partition index, relevantIndexList)
   *          where relevantIndexList(i) = the index
   *          of an element on this partition that matches one of the target ranks.
   */
  //tag::firstTry_Step3[]
  private def getRanksLocationsWithinEachPart(targetRanks : List[Long],
         partitionColumnsFreq : Array[(Int, Array[Long])],
         numOfColumns : Int) : Array[(Int, List[(Int, Long)])] = {

    val runningTotal = Array.fill[Long](numOfColumns)(0)
    // The partition indices are not necessarily in sorted order, so we need
    // to sort the partitionsColumnsFreq array by the partition index (the
    // first value in the tuple).
    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq) =>
      val relevantIndexList = new MutableList[(Int, Long)]()

      columnsFreq.zipWithIndex.foreach{ case (colCount, colIndex)  =>
        val runningTotalCol = runningTotal(colIndex)
        val ranksHere: List[Long] = targetRanks.filter(rank =>
          runningTotalCol < rank && runningTotalCol + colCount >= rank)

        // For each of the rank statistics present add this column index and the
        // index it will be at on this partition (the rank - the running total).
        relevantIndexList ++= ranksHere.map(
          rank => (colIndex, rank - runningTotalCol))

        runningTotal(colIndex) += colCount
      }

      (partitionIndex, relevantIndexList.toList)
    }
  }
  //end::firstTry_Step3[]

  /**
    * Step 4: Finds rank statistics elements using ranksLocations.
    *
    * @param sortedValueColumnPairs - sorted RDD of (value, colIndex) pairs
    * @param ranksLocations Array of (partition Index, list of (column index,
    *                       rank index of this column at this partition))
    *
    * @return returns RDD of the target ranks (column index, value)
    */
  //tag::firstTry_Step4[]
  private def findTargetRanksIteratively(
    sortedValueColumnPairs : RDD[(Double, Int)],
    ranksLocations : Array[(Int, List[(Int, Long)])]):
      RDD[(Int, Double)] = {

    sortedValueColumnPairs.mapPartitionsWithIndex(
      (partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) => {
        val targetsInThisPart: List[(Int, Long)] = ranksLocations(partitionIndex)._2
        if (targetsInThisPart.nonEmpty) {
          val columnsRelativeIndex: Map[Int, List[Long]] =
          targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
          val columnsInThisPart = targetsInThisPart.map(_._1).distinct

          val runningTotals : mutable.HashMap[Int, Long]=  new mutable.HashMap()
          runningTotals ++= columnsInThisPart.map(
            columnIndex => (columnIndex, 0L)).toMap

  //filter this iterator, so that it contains only those (value, columnIndex)
  //that are the ranks statistics on this partition
  //Keep track of the number of elements we have seen for each columnIndex using the
  //running total hashMap.
        valueColumnPairs.filter{
          case(value, colIndex) =>
            lazy val thisPairIsTheRankStatistic: Boolean = {
              val total = runningTotals(colIndex) + 1L
              runningTotals.update(colIndex, total)
              columnsRelativeIndex(colIndex).contains(total)
            }
             (runningTotals contains colIndex) && thisPairIsTheRankStatistic
        }.map(_.swap)
      }
      else {
        Iterator.empty
      }
    })
  }
  //end::firstTry_Step4[]
}
