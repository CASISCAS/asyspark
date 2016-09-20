package org.apache.spark.examples

import org.apache.spark.asysgd.AsyGradientDescent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{GradientDescent, LogisticGradient, SimpleUpdater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by wjf on 16-9-19.
  */
object AsySGDExample {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val initialB = -1.0
    val initialWeights = Array(initialB)

    val gradient = new LogisticGradient()
    val updater = new SimpleUpdater()
    val stepSize = 1.0
    val numIterations = 1000
    val regParam = 0
    val miniBatchFrac = 1.0
    val convergenceTolerance = 5.0e-1

    // Add a extra variable consisting of all 1.0's for the intercept.
    val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)

    val data = testData.map { case LabeledPoint(label, features) =>
      label -> MLUtils.appendBias(features)
    }

    val dataRDD = sc.parallelize(data, 10).cache()
    val initialWeightsWithIntercept = Vectors.dense(initialWeights.toArray :+ 1.0)

    // our asychronous implement
    var start = System.nanoTime()
    val (weights, weightHistory) = AsyGradientDescent.runAsySGD(
      dataRDD,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept,
      convergenceTolerance)
    var end = System.nanoTime()
    println((end - start) / 1e6 +"ms")
    weights.toArray.foreach(println)
    // use spark implement
    start = System.nanoTime()
    val (weight, weightHistorys) = GradientDescent.runMiniBatchSGD( dataRDD,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept,
      convergenceTolerance)
    end = System.nanoTime()
    println((end - start)/ 1e6 + "ms")
    weight.toArray.foreach(println)
  }

}
object GradientDescentSuite {

  def generateLogisticInputAsList(
                                   offset: Double,
                                   scale: Double,
                                   nPoints: Int,
                                   seed: Int): java.util.List[LabeledPoint] = {
    generateGDInput(offset, scale, nPoints, seed).asJava
  }

  // Generate input of the form Y = logistic(offset + scale * X)
  def generateGDInput(
                       offset: Double,
                       scale: Double,
                       nPoints: Int,
                       seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val unifRand = new Random(45)
    val rLogis = (0 until nPoints).map { i =>
      val u = unifRand.nextDouble()
      math.log(u) - math.log(1.0-u)
    }

    val y: Seq[Int] = (0 until nPoints).map { i =>
      val yVal = offset + scale * x1(i) + rLogis(i)
      if (yVal > 0) 1 else 0
    }
    (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(x1(i))))
  }
}