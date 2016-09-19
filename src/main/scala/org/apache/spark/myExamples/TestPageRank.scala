package org.apache.spark.myExamples

import org.apache.spark.sql.SparkSession

/**
  * Created by wjf on 16-8-30.
  */
object TestPageRank {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("test PageRank").master("local").getOrCreate()

    val iter=10
    val links = spark.read.textFile("data/mllib/pagerank_data.txt").rdd.map { line =>
      val  parts = line.split(" ")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    println(links.partitions.length)
    println(links.toDebugString)

    links.foreach(println)

    // see the source code ,i find that spark rdd action will do CheckPoint() automatically
//    links.foreach(println)

    var ranks = links.mapValues( v => 1.0)


    ranks.foreach(println)

    for(i <- 1 to iter){
      val contri=links.join(ranks).values.flatMap{ case (urls,rank) =>
        val size=urls.size
        urls.map(url => (url,rank/size))
      }
      ranks=contri.reduceByKey(_ + _).mapValues(0.15 + 0.85*_)
      println("Iteration %d".format(i))
      ranks.foreach(println)
    }

  }

}
