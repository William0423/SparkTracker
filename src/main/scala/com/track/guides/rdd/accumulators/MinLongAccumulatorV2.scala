package com.track.guides.rdd.accumulators

import org.apache.spark.util.AccumulatorV2

class MinLongAccumulatorV2 extends AccumulatorV2[Long, Long]{ // 第一个为输入参数， 第二个为输出参数

  private var currentVal = Long.MaxValue

  override def isZero: Boolean = {currentVal == Long.MaxValue}

  override def copy(): AccumulatorV2[Long, Long] = {
    val newMinLongAcc = new MinLongAccumulatorV2
    newMinLongAcc.currentVal = this.currentVal
    newMinLongAcc
  }

  override def reset(): Unit = currentVal = Long.MaxValue

  override def add(v: Long): Unit = currentVal = Math.min(v, currentVal)

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o :MinLongAccumulatorV2 => add(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Long = currentVal
}
