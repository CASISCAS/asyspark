package org.apache.spark.myExamples

import org.apache.spark.ml.feature.LabeledPoint
import java.util.{Comparator, PriorityQueue => JPriorityQueue}

import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable.HashMap
/**
  * Created by wjf on 16-8-28.
  */
object KNN {
  def main(args: Array[String]): Unit = {
    val knn = new KNN(3)

    val points =Vector[LabeledPoint](LabeledPoint(1.0,Vectors.dense(1.0,2.0,3.0)),LabeledPoint(1.0,Vectors.dense(1.0,2.0,3.0)),LabeledPoint(0.0,Vectors.dense(1.0,2.0,3.0)))

    val testPoint=LabeledPoint(1,Vectors.dense(1.0,2.0,3.0))
    println(knn.fit(points,testPoint))

  }
}

class KNN(var n:Int =2) extends Logging{

  def set(n:Int): Unit ={
    this.n=n
  }
  def fit(labeledPoint:Vector[LabeledPoint],testPoint: LabeledPoint): Double ={
    val priorityQ= new JPriorityQueue[(Double,Int)](math.min(100,labeledPoint.size),new Comparator[(Double, Int)] {
      override def compare(o1: (Double, Int), o2: (Double, Int)): Int = {
        if(o1._1 == o2._1){
          o1._2.compareTo(o2._2)
        }
        else{
          o1._1.compareTo(o1._1)
        }
      }
    })
    (0 until n).foreach(i => {
        val dis = compute_dis(labeledPoint(i).features.toArray, testPoint.features.toArray)
        priorityQ.add((dis, i))
      }
    )
    var maxDis=priorityQ.peek()._1
    println(priorityQ.size())
    println(priorityQ.size())

    Seq(priorityQ.element()).foreach(println)
    println("element")
    // traverse every element in the test dataset
    labeledPoint.indices.foreach(i =>{
      val tempDis=compute_dis(labeledPoint(i).features.toArray,testPoint.features.toArray)
      if(tempDis < maxDis){
        priorityQ.poll()
        priorityQ.add((tempDis,i))
        maxDis = priorityQ.peek()._1
      }
    })
    val count=new HashMap[Double,Int]
    val iter=priorityQ.iterator()
    while(iter.hasNext){
      val index=iter.next()._2
      if(count.contains( labeledPoint(index).label)){

        count(labeledPoint(index).label) = count(labeledPoint(index).label) + 1
      }
      else{
        count(labeledPoint(index).label)=1
      }

    }
    count.foreach(println)



    val (label,_)=count.toList.max(new  Ordering[(Double,Int)]{
      override def compare(x: (Double, Int), y: (Double, Int)): Int = x._2.compareTo(y._2)
    })
//      .maxBy(new Ordering[(Double,Int)]{
//      override def compare(x: (Double, Int), y: (Double, Int)): Int = x._2.compareTo(y._2)
//    } )
    label

  }
  // sqrt distance
  def compute_dis(v1:Array[Double],v2:Array[Double]): Double ={
    var sum:Double =0.0
    if(v1.length != v2.length){
      logError("Error:length of v1 not equals v2!")
    }
    v1.indices.foreach( x => sum = sum + (v1(x) * v2(x)))
    math.sqrt(sum)
  }
}
