package org.apache.spark.asysgd

/**
  * Created by wjf on 16-9-19.
  */
class WeightUtils {
  // some methods to operate weights

  def addFromTo(from: List[Weight], to: List[Weight]): Unit = {
    require(from.size == to.size, s"from.size must equals to.size, but got ${from.size} ${to.size}")

    for(i <- 0 until from.size) {
      val f = from(i)
      val t = to(i)
      for(j <- f.offset until f.data.length) {
        t.data(j) += f.data(j - f.offset)
      }
    }
  }

}
