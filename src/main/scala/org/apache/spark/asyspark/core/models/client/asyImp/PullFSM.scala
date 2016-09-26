package org.apache.spark.asyspark.core.models.client.asyImp


import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.spark.asyspark.core.Exceptios.PullFailedException

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
  * Created by wjf on 16-9-25.
  */
class PullFSM[T, R: ClassTag](message: T,
                              actorRef: ActorRef,
                              maxAttempts: Int,
                              initialTimeout: FiniteDuration,
                              maxTimeout: FiniteDuration,
                              backoffFactor: Double)(implicit ec: ExecutionContext) {
  private implicit var timeout: Timeout = new Timeout(initialTimeout)

  private var attempts = 0
  private var runflag =false
  private val promise: Promise[R] = Promise[R]()

  def run(): Future[R] ={
    if(!runflag) {
      execute()
      runflag = true
    }
    promise.future
  }

  private def execute(): Unit ={
    if(attempts < maxAttempts) {
      attempts += 1
      request()
    } else {
      promise.failure(new PullFailedException(s"Failed $attempts while the maxAttempts is $maxAttempts to pull data"))
    }
  }

  private def request(): Unit ={
    val request = actorRef ? message
    request.onFailure {
      case ex: AskTimeoutException =>
        timeBackoff()
        execute()
      case _ =>
        execute()
    }
    request.onSuccess {
      case response: R =>
        promise.success(response)
      case _ =>
        execute()
    }
  }

  private def timeBackoff(): Unit ={

    if(timeout.duration.toMillis * backoffFactor > maxTimeout.toMillis) {
      timeout = new Timeout(maxTimeout)
    } else {
      timeout = new Timeout(timeout.duration.toMillis * backoffFactor millis)
    }
  }



}

object PullFSM {

  private var maxAttempts: Int = 10
  private var initialTimeout: FiniteDuration = 5 seconds
  private var maxTimeout: FiniteDuration = 5 minutes
  private var backoffFactor: Double = 1.6

  /**
    * Initializes the FSM default parameters with those specified in given config
    * @param config The configuration to use
    */
  def initialize(config: Config): Unit = {
    maxAttempts = config.getInt("asyspark.pull.maximum-attempts")
    initialTimeout = new FiniteDuration(config.getDuration("asyspark.pull.initial-timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)
    maxTimeout = new FiniteDuration(config.getDuration("asyspark.pull.maximum-timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)
    backoffFactor = config.getInt("asyspark.pull.backoff-multiplier")
  }

  /**
    * Constructs a new FSM for given message and actor
    *
    * @param message The pull message to send
    * @param actorRef The actor to send to
    * @param ec The execution context
    * @tparam T The type of message to send
    * @return An new and initialized PullFSM
    */
  def apply[T, R : ClassTag](message: T, actorRef: ActorRef)(implicit ec: ExecutionContext): PullFSM[T, R] = {
    new PullFSM[T, R](message, actorRef, maxAttempts, initialTimeout, maxTimeout, backoffFactor)
  }

}
