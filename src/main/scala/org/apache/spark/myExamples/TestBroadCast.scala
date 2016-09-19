package org.apache.spark.myExamples

import org.apache.spark.sql.SparkSession

/**
  * Created by wjf on 16-8-30.
  */
object TestBroadCast {
  def main(args: Array[String]): Unit = {
    val blockSize = if(args.length >2) args(2) else "4096"
    val spark =SparkSession.builder().appName("testBroadCast").config("spark.broadcast.blockSize",blockSize).master("local").getOrCreate()

    val sc = spark.sparkContext

    val slice = if(args.length >0) args(0).toInt else 2
    val num = if(args.length >1) args(1).toInt else 10000

    val arr1=(1 to num).toArray

    for(i <- 1 to 3){
      println("Iteration " + i)
      println("--------------------------")
      val start= System.nanoTime()
      val broad = sc.broadcast(arr1)
      val numSize= sc.parallelize(1 to 10,slice).map(_ => broad.value.length)
      numSize.collect().foreach(println)

      println("Iteration %d take %.0f milliseconds".format(i,(System.nanoTime()-start)/1E6) )
    }

  }

}
