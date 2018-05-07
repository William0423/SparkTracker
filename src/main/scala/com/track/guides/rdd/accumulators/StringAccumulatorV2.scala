package com.track.guides.rdd.accumulators

import org.apache.spark.util.AccumulatorV2

class StringAccumulatorV2 extends AccumulatorV2[String, String] { // 第一个为输入参数， 第二个为输出参数

  private var res = ""

  override def isZero: Boolean = {res == ""}

  override def copy(): AccumulatorV2[String, String] = {
    val newStringAcc = new StringAccumulatorV2
    newStringAcc.res = this.res
    newStringAcc
  }

  override def reset(): Unit = res = ""

  override def add(v: String): Unit = res += v

  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case o : StringAccumulatorV2 => res += o.res
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: String = res
}
