package org.apache.spark.myExamples

import java.util.{Comparator, PriorityQueue => JPriorityQueue}

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.BoundedPriorityQueue

import scala.collection.mutable.Map

/**
  * Created by wjf on 16-8-29.
  */

object SparkKNN{

  def showWarning(): Unit ={
    System.err.println(
      """WARN:This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use ,Oh sorry there is no implement in ml.clustering
      """.stripMargin
    )
  }
  def compute_dis(v1:Array[Double],v2:Array[Double]): Double ={
    var sum:Double =0.0
    if(v1.length != v2.length){
      System.err.println("Error:length of v1 not equals v2!")
    }
    v1.indices.foreach( x => sum = sum + (v1(x) * v2(x)))
    math.sqrt(sum)
  }
  def main(args: Array[String]): Unit = {
    showWarning()
    val spark = SparkSession.builder().appName("SparkKNN").master("local").getOrCreate()

    val lines=spark.read.textFile("data/mllib/KNN_data.txt").rdd
    val data=lines.map{line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()


    val k=3
    val label=fit(data,LabeledPoint(0,Vectors.dense(1.0,2.0,3.0)),k)
    println("label: "+label)

  }
  def fit(data:RDD[LabeledPoint], labeledPoint:LabeledPoint, k:Int):Double ={
//    val priorityQ= new SerializablePriorityQueue[(Double,Double)](math.min(100,data.count().toInt))(new Ordering[(Double, Double)] {
//      override def compare(o1: (Double, Double), o2: (Double,Double)): Int = {
//        if(o1._1 == o2._1){
//          o1._2.compareTo(o2._2)
//        }
//        else{
//          o1._1.compareTo(o1._1)
//        }
//      }
//    })
//    val disData =data.map{ point => {
//      val dis = compute_dis(point.features.toArray, labeledPoint.features.toArray)
//      println(dis,point.label)
//      (dis, point.label)
//
//    }}.collect()

    val disData =data.mapPartitions{
      parts => {
        var disSeq: List[(Double, Double)] = List[(Double, Double)]()
        while (parts.hasNext) {
          val tempPoint = parts.next()
          val dis = compute_dis(tempPoint.features.toArray, labeledPoint.features.toArray)
          disSeq = (dis, tempPoint.label) :: disSeq
        }


        disSeq=disSeq.sorted[(Double,Double)](new Ordering[(Double, Double)]() {
          override def compare(x: (Double, Double), y: (Double, Double)): Int =
            if( x._1 == y._1) x._2.compareTo(y._2)
            else x._1.compareTo(y._1)

        })
        disSeq.slice(0 ,k).iterator
      }
    }.collect()

    disData.sorted(new Ordering[(Double,Double)](){
      override def compare(x: (Double, Double), y: (Double, Double)): Int =
        if( x._1 == y._1) x._2.compareTo(y._2)
        else x._1.compareTo(y._1)
    })
    val knnData=data.context.parallelize(disData.take(k).map(x =>(x._2,1)))
    val combinedData=knnData.aggregateByKey(0)(_ + _, (x,y) =>math.max(x,y))
    val (label:Double,_)=combinedData.max()(new Ordering[(Double,Int)]{
      override def compare(x: (Double, Int), y: (Double, Int)): Int = x._2.compareTo(y._2)
    })
    label




//    var maxDis=priorityQ.getPQ.peek()._1

//
//    data.map{ point => {
//      val tempDis = compute_dis(point.features.toArray, labeledPoint.features.toArray)
//      if (tempDis < maxDis) {
//        priorityQ.getPQ.poll()
//        priorityQ.getPQ.add((tempDis, point.label))
//        maxDis = priorityQ.getPQ.peek()._1
//      }
//    }}.collectAsMap()
//    priorityQ.getPQ.toArray.foreach(println)
////
//    val (_,label:Double)=getPQ.value.toArray[(_,Double,Double)]().max(new Ordering[(_,Double,Double)]{
//      override def compare(x: (_,Double, Double), y: (_,Double, Double)): Int =
//        if(x._3 == y._3){
//          x._2.compareTo(y._2)
//        }
//        else{
//          x._3.compareTo(y._3)
//        }
//    })
//    getPQ.destroy()


  }
}
