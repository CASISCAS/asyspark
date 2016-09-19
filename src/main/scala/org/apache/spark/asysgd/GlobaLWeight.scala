package org.apache.spark.asysgd

import breeze.linalg.{axpy => brzAxpy, norm => brzNorm, Vector => BV, norm}

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by wjf on 16-9-19.
  */
object GlobaLWeight {
  //TODO this is just a damo, will use a cluster to replace this
  private var weight = Vector.empty
  private var regVal = 0.0

  def getWeight(): Vector = {
    weight
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
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzWeights :*= (1.0 - thisIterStepSize * regParam)
    brzAxpy(-thisIterStepSize, gradient.asBreeze, brzWeights)
    val norm = brzNorm(brzWeights, 2.0)
    val currentWeight = Vectors.fromBreeze(brzWeights)
    val flag = isConverged(weightsOld, currentWeight, convergenceTol)
    this.weight = Vectors.fromBreeze(brzWeights)
    this.regel = 0.5 * regParam * norm * norm
    (true, flag)
  }

  private def isConverged(
                           previousWeights: Vector,
                           currentWeights: Vector,
                           convergenceTol: Double): Boolean = {
    // To compare with convergence tolerance.
    val previousBDV = previousWeights.asBreeze.toDenseVector
    val currentBDV = currentWeights.asBreeze.toDenseVector

    // This represents the difference of updated weights in the iteration.
    val solutionVecDiff: Double = norm(previousBDV - currentBDV)

    solutionVecDiff < convergenceTol * Math.max(norm(currentBDV), 1.0)
  }

}
