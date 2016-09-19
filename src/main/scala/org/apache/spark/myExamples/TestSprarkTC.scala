package org.apache.spark.myExamples

import org.apache.spark.sql.SparkSession

import scala.util.Random
import scala.collection.mutable.Set
/**
  * Created by wjf on 16-8-31.
  */
object TestSprarkTC {

  val numVectices =100
  val numEdges =200
  val rand = new Random(42)

  def generateData():Seq[(Int,Int)]={
    val edges :Set[(Int,Int)]=Set.empty
    while(edges.size < numEdges){
      val from =rand.nextInt(numVectices)
      val to =rand.nextInt(numVectices)
      if(from != to) edges.add(from,to)
    }
    edges.toSeq

  }

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("test saprk tc").getOrCreate()
    val sc =spark.sparkContext
    val numSlices=3
    var tc= sc.parallelize(generateData(),numSlices).cache()
    val edges =tc.map(x => (x._2,x._1))
    var oldCount =0L
    var newCount =tc.count()
    do {
      oldCount=newCount
      tc=tc.union( tc.join(edges).map(x => (x._2._2,x._2._1))).distinct().cache()
      newCount=tc.count()
    } while(oldCount != newCount)
    println("TC has %d edges".format(tc.count()) )
    sc.stop()
  }

}
