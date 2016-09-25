package org.apache.spark.examples

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Created by wjf on 16-9-24.
  */
object TestBroadCast extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("test BoradCast").getOrCreate()
    val sc = sparkSession.sparkContext
    //    val data = sc.parallelize(Seq(1 until 10000000))
    val num = args(args.length - 2).toInt
    val times = args(args.length -1).toInt
    println(num)
    val start = System.nanoTime()
    val seq =Seq(1 until num)
    for(i <- 0 until times) {
      val start2 = System.nanoTime()
      val bc = sc.broadcast(seq)
      val rdd = sc.parallelize(1 until 10, 5)
      rdd.map(_ => bc.value.take(1)).collect()
      println((System.nanoTime() - start2)/ 1e6 + "ms")
    }
    logInfo((System.nanoTime() - start) / 1e6 + "ms")
  }
}
