package org.apache.spark.examples

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by wjf on 16-9-24.
  */
object TestBroadCast extends Logging{
  val sparkSession = SparkSession.builder().appName("test BoradCast").getOrCreate()
  val sc = sparkSession.sparkContext
  def main(args: Array[String]): Unit = {

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

  def testMap(): Unit ={

    val smallRDD = sc.parallelize(Seq(1,2,3))
    val bigRDD = sc.parallelize(Seq(1 until 20))

    bigRDD.mapPartitions {
      partition =>
        val hashMap = new mutable.HashMap[Int,Int]()
        for(ele <- smallRDD) {
          hashMap(ele) = ele
        }
        // some operation here
        partition

    }
  }
}
