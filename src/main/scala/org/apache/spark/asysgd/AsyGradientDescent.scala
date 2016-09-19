package org.apache.spark.asysgd

import breeze.linalg.{norm, DenseVector => BDV}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.GradientDescent._
import org.apache.spark.mllib.optimization.{Gradient, Updater}
import org.apache.spark.rdd.RDD
import sun.util.logging.resources.logging

import scala.collection.mutable.ArrayBuffer
import scala.reflect.internal.util.Collections

/**
  * Created by wjf on 16-9-19.
  */
object AsyGradientDescent extends Logging {
  /**
    * Run asychronous stochastic gradient descent (SGD) in parallel using mini batches.
    * In each iteration, we sample a subset (fraction miniBatchFraction) of the total data
    * in order to compute a gradient estimate.
    * Sampling, and averaging the subgradients over this subset is performed using one standard
    * spark map-reduce in each iteration.
    *
    * @param data Input data for SGD. RDD of the set of data examples, each of
    *             the form (label, [feature values]).
    * @param gradient Gradient object (used to compute the gradient of the loss function of
    *                 one single data example)
    * @param updater Updater function to actually perform a gradient step in a given direction.
    * @param stepSize initial step size for the first step
    * @param numIterations number of iterations that SGD should be run.
    * @param regParam regularization parameter
    * @param miniBatchFraction fraction of the input data set that should be used for
    *                          one iteration of SGD. Default value 1.0.
    * @param convergenceTol Minibatch iteration will end before numIterations if the relative
    *                       difference between the current weight and the previous weight is less
    *                       than this value. In measuring convergence, L2 norm is calculated.
    *                       Default value 0.001. Must be between 0.0 and 1.0 inclusively.
    * @return A tuple containing two elements. The first element is a column matrix containing
    *         weights for every feature, and the second element is an array containing the
    *         stochastic loss computed for every iteration.
    */
  def runAsySGD(
                       data: RDD[(Double, Vector)],
                       gradient: Gradient,
                       updater: Updater,
                       stepSize: Double,
                       numIterations: Int,
                       regParam: Double,
                       miniBatchFraction: Double,
                       initialWeights: Vector,
                       convergenceTol: Double): (Vector, Array[Double]) = {

    // convergenceTol should be set with non minibatch settings
    if (miniBatchFraction < 1.0 && convergenceTol > 0.0) {
      logWarning("Testing against a convergenceTol when using miniBatchFraction " +
        "< 1.0 can be unstable because of the stochasticity in sampling.")
    }

    if (numIterations * miniBatchFraction < 1.0) {
      logWarning("Not all examples will be used if numIterations * miniBatchFraction < 1.0: " +
        s"numIterations=$numIterations and miniBatchFraction=$miniBatchFraction")
    }

    val stochasticLossHistory = new ArrayBuffer[Double](numIterations)
    // Record previous weight and current one to calculate solution vector difference

    var previousWeights: Option[Vector] = None
    var currentWeights: Option[Vector] = None

    val numExamples = data.count()

    // if no data, return initial weights to avoid NaNs
    if (numExamples == 0) {
      logWarning("GradientDescent.runMiniBatchSGD returning initial weights, no data found")
      return (initialWeights, stochasticLossHistory.toArray)
    }

    if (numExamples * miniBatchFraction < 1) {
      logWarning("The miniBatchFraction is too small")
    }

    // Initialize weights as a column vector
    val weights = Vectors.dense(initialWeights.toArray)
    val n = weights.size

    /**
      * For the first iteration, the regVal will be initialized as sum of weight squares
      * if it's L2 updater; for L1 updater, the same logic is followed.
      */
    var regVal = updater.compute(
      weights, Vectors.zeros(weights.size), 0, 1, regParam)._2

    var converged = false // indicates whether converged based on convergenceTol
    var i = 1

    data.foreachPartition {
      partition =>
        val coveraged = false
        val i = 0
        while (i < numIterations && !coveraged) {
          val loss = 0
          // todo can do some optimization
          //          val timesPerIter =10
          //          for(j <- 0 until timesPerIter) {
          //
          //          }
          val bcWeight = GlobaLWeight.weight
          // todo we can do a sample to avoid use all the data

          // compute gradient
          val (gradientSum, lossSum) = partition.aggregate((BDV.zeros[Double](n), 0.0))(
            seqop = (c, v) => {
              val l = gradient.compute(v._2, v._1, bcWeight, Vectors.fromBreeze(c._1))
              (c._1, c._2 + l)
            },
            comOp = (c1, c2) => {
              (c1._1 += c2._1, c1._2 + c2._2)
            }
          )
          // update gradient
          val elementNum = partition.count()
          if (elementNum > 0) {
            stochasticLossHistory += lossSum / elementNum
            // todo check whether update success
            val (success , convergence) = GlobaLWeight.updateWeight(weights, Vectors.fromBreeze(gradientSum / elementNum), stepSize, i, regParam, convergenceTol)

            if(convergence) {
              converged = true
            }
          } else {
            logWarning(s" sorry, this partition has no elements, this worker will stop")
            converged = true
          }
          i += 1
        }
    }
  }




}
