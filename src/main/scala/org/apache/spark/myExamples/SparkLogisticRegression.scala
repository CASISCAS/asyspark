package org.apache.spark.myExamples

import breeze.linalg.{DenseVector,Vector}
import breeze.numerics.exp
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Created by wjf on 16-8-30.
  */
object SparkLogisticRegression {
  val N =10000
  val D =10
  val R=0.7
  val Iter=10
  val rand=new Random(42)

  case class DataPoint(x:Vector[Double],y:Double)

  def generateData(): Array[DataPoint] ={
    def generatePoint(i:Int):DataPoint={
      val y=if(i%2 ==0) -1 else 1
      val x=DenseVector.fill(D){rand.nextGaussian() + R*y}
      DataPoint(x,y)
    }
    Array.tabulate(N)(generatePoint)
  }
  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().appName("test LogisticRegression").master("local").getOrCreate()
    /*this is just a naive implementation
    * */
    val sc=spark.sparkContext

    val points=sc.parallelize(generateData(),2).cache()

    val w=DenseVector.fill(D){2* rand.nextDouble -1}
    for(i <- 0 until Iter){
      println("Iteration %d ".format(i))
      val gradient = points.map{point =>
        point.x * ( 1 /( 1+ exp(w.dot(point.x)))- point.y)
      }.reduce(_ + _)
      w -= gradient
    }
    println("Final w: "+w)


  }

}
