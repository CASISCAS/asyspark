package org.apache.spark.asysgd

import breeze.linalg.{Vector => BV, axpy => brzAxpy, norm => brzNorm}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
  * Created by wjf on 16-9-19.
  */

object GlobalWeight extends Serializable{
  //TODO this is just a damo, will use a cluster to replace this
  //TODO the init weight can be optimized
  //TODO concurrent control should be optimized
  private var n: Int = 0
  private var weight: Vector = _
  private var regVal: Double = 0.0
  // must setN first before use GloabalWeight
  def setN(n: Int): Unit ={
    this.n = n
  }
  def initWeight(): Vector = {
    GlobalWeight.synchronized {
      if(this.weight == null) {
        this.weight = Vectors.dense(new Array[Double](n))
      }
      this.weight
    }
  }
  def setWeight(weight:Vector):Boolean = {
    this.weight = weight
    true
  }
  def getWeight(): Vector = {
    require(n > 0 ,s"you must set n before useing GlabelWeight, temp n is ${n}")
    this.weight
  }

  def updateWeight(
                        weightsOld: Vector,
                        gradient: Vector,
                        stepSize: Double,
                        iter: Int,
                        regParam: Double, convergenceTol: Double): (Boolean, Boolean) = {
    // add up both updates from the gradient of the loss (= step) as well as
    // the gradient of the regularizer (= regParam * weightsOld)
    // w' = w - thisIterStepSize * (gradient + regParam * w)
    // w' = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient

    GlobalWeight.synchronized {
      val thisIterStepSize = stepSize / math.sqrt(iter)
      val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
      brzWeights :*= (1.0 - thisIterStepSize * regParam)
      brzAxpy(-thisIterStepSize, gradient.asBreeze, brzWeights)
      val norm = brzNorm(brzWeights, 2.0)
      val currentWeight = Vectors.fromBreeze(brzWeights)
      val flag = isConverged(weightsOld, currentWeight, convergenceTol)
      this.weight = Vectors.fromBreeze(brzWeights)
      this.regVal = 0.5 * regParam * norm * norm
      (true, flag)
    }
  }
  private def isConverged(
                           previousWeights: Vector,
                           currentWeights: Vector,
                           convergenceTol: Double): Boolean = {
    // To compare with convergence tolerance.
    val previousBDV = previousWeights.asBreeze.toDenseVector
    val currentBDV = currentWeights.asBreeze.toDenseVector

    // This represents the difference of updated weights in the iteration.
    val solutionVecDiff: Double = brzNorm(previousBDV - currentBDV)

    solutionVecDiff < convergenceTol * Math.max(brzNorm(currentBDV), 1.0)
  }
}
