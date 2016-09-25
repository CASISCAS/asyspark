package org.apache.spark.asyspark.core.models.client.asyImp

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.spark.asyspark.core.Exceptios.PushFailedException
import org.apache.spark.asyspark.core.messages.server.logic._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}


/**
  * A push-mechanism using a finite state machine to guarantee exactly-once delivery with multiple attempts
  *
  * @param message A function that takes an identifier and generates a message of type T
  * @param actorRef The actor to send to
  * @param maxAttempts The maximum number of attempts
  * @param maxLogicAttempts The maximum number of attempts to establish communication through logic channels
  * @param initialTimeout The initial timeout for the request
  * @param maxTimeout The maximum timeout for the request
  * @param backoffFactor The backoff multiplier
  * @param ec The execution context
  * @tparam T The type of message to send
  * @author wjf created 16-9-25
  */
class PushFSM[T](message: Int => T,
                 actorRef: ActorRef,
                 maxAttempts: Int,
                 maxLogicAttempts: Int,
                 initialTimeout: FiniteDuration,
                 maxTimeout: FiniteDuration,
                 backoffFactor: Double)(implicit ec: ExecutionContext) {

  private implicit var timeout: Timeout = new Timeout(initialTimeout)

  /**
    * Unique identifier for this push
    */
  private var id =0

  /**
    * Counter for the number of push attempts
    */
  private var attempts = 0

  private var logicAttempts = 0

  /**
    * Flag to make sure only one request at some moment
    */
  private var runFlag = false

  private val promise: Promise[Boolean] = Promise[Boolean]()

  /**
    * Run the push request
    * @return
    */
  def run(): Future[Boolean] = {
    if(!runFlag) {
      prepare()
      runFlag = true
    }
    promise.future
  }

  /**
    * Prepare state
    * obtain a unique and available identifier from the parameter server for the next push request
    */
  private def prepare(): Unit ={
    val prepareFuture = actorRef ? GenerateUniqueID()
    prepareFuture.onSuccess {
      case UniqueID(identifier) =>
        id = identifier
        execute()
      case _ =>
        retry(prepare)
    }
    prepareFuture.onFailure {
      case ex: AskTimeoutException =>
        timeBackoff()
        retry(prepare)
      case _ =>
        retry(prepare)
    }
  }

  /**
    * Execute the push request performing a single push
    */
  private def execute(): Unit = {
    if(attempts >= maxAttempts) {
      promise.failure(new PushFailedException(s"Failed ${attempts} is equal to  maxAttempts ${maxAttempts} to push data"))
    } else {
      attempts += 1
      actorRef ! message(id)
      acknowledge()
    }
  }

  /**
    * Acknowledge state
    * We keep sending acknowledge messages until we either receive a acknowledge or notAcknowledge
    */
  private def acknowledge(): Unit ={
    val ackFuture = actorRef ? AcknowledgeReceipt(id)
    ackFuture.onSuccess {
      case NotAcknowledgeReceipt(identifier) if identifier == id =>
        execute()
      case AcknowledgeReceipt(identifier) if identifier == id =>
        promise.success(true)
        forget()
      case _ =>
        retry(acknowledge)
    }
    ackFuture.onFailure {
      case ex:AskTimeoutException =>
        timeBackoff()
        retry(acknowledge)
      case _ =>
        retry(acknowledge)
    }

  }

  /**
    * Forget state
    * We keep sending forget messages until we receive a successful reply
    */
  private def forget(): Unit ={
    val forgetFuture = actorRef ? Forget(id)
    forgetFuture.onSuccess {
      case Forget(identifier) if identifier == id =>
        ()
      case _ =>
        forget()
    }
    forgetFuture.onFailure {
      case ex: AskTimeoutException =>
        timeBackoff()
        forget()
      case _ =>
        forget()
    }

  }

  /**
    * Increase the timeout with an exponential backoff
    */
  private def timeBackoff(): Unit ={
   if(timeout.duration.toMillis * backoffFactor > maxTimeout.toMillis) {
     timeout = new Timeout(maxTimeout)
   } else {
     timeout = new Timeout(timeout.duration.toMillis * backoffFactor millis)
   }
  }

  /**
    * Retries a function while keeping track of a logic attempts counter and fails when the logic attemps counter is
    * too large
    * @param func The function to execute again
    */
  private def retry(func: () => Unit): Unit ={
    logicAttempts += 1
    if (logicAttempts < maxLogicAttempts) {
      func()
    } else {
      promise.failure(new PushFailedException(s"Failed $logicAttempts time while the maxLogicAttemps is $maxLogicAttempts "))
    }
  }
}
object PushFSM {
  private var maxAttempts: Int = 10
  private var maxLogicAttempts: Int = 100
  private var initialTimeout: FiniteDuration = 5 seconds
  private var maxTimeout: FiniteDuration = 5 minutes
  private var backoffFactor: Double = 1.6

  /**
    * Initialize the FSM from the default config file
    * @param config
    */
  def initialize(config: Config): Unit ={
    maxAttempts = config.getInt("asyspark.push.maximum-attempts")
    maxLogicAttempts = config.getInt("asyspark.push.maximum-logic-attempts")
    initialTimeout = new FiniteDuration(config.getDuration("asyspark.push.initial-timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
    maxTimeout = new FiniteDuration(config.getDuration("asyspark.push.maximum-timeout", TimeUnit.MICROSECONDS),
      TimeUnit.MICROSECONDS)
    backoffFactor = config.getDouble("asyspark.push.backoff-multiplier")
  }

  /**
    * construct a fsm
    * @param message
    * @param actorRef
    * @param ec
    * @tparam T
    * @return
    */
  def apply[T](message: Int => T, actorRef: ActorRef)(implicit ec: ExecutionContext): PushFSM[T] =
    new PushFSM(message, actorRef, maxAttempts, maxLogicAttempts, initialTimeout, maxTimeout, backoffFactor)

}
