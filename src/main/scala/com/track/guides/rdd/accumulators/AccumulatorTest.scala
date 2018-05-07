package com.track.guides.rdd.accumulators

object AccumulatorTest {

  def getMatchType(msg:Any){
    msg match {
      case i : Int=>println("Integer")
      case s : String=>println("String")
      case d : Double=>println("Double")
      case l : Long=>println("Long = " + l)
      case _=>println("Unknow type")
    }
  }

  def main(args: Array[String]): Unit = {

    val mst = Long.MaxValue
    getMatchType(mst)

//    val sparkS = SparkSession.builder().master("local[2]").getOrCreate()
//    val sc = sparkS.sparkContext
////    val strAcc = new StringAccumulatorV2
////    sc.register(strAcc, "offsets")
//    val longAcc = new MinLongAccumulatorV2
//    sc.register(longAcc, "longAcc")
//
//    val sum = sc.parallelize(List(100L,23L,3L,4L,5L,63L,7L), 2)
//
////    sum.map(x=>longAcc.add(x))
////    println(longAcc.value)
////    sum.count()
//    sum.foreach(x => longAcc.add(x))
//    println(longAcc.value)
//    val copyAcc = longAcc.copy()
//    copyAcc.reset()
//    println(copyAcc.value)
//    println()




    println(Long.MaxValue.equals(Long.MaxValue))


  }

}
