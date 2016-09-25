package org.apache.spark.asyspark.core.models.server

import akka.actor.ActorRef
import org.apache.spark.asyspark.core.messages.server.logic._

import scala.collection.mutable

/** Some common push logic behavior
  * Created by wjf on 16-9-25.
  */
trait PushLogic {
  /**
    * A set of received message ids
    */
  val receipt : mutable.HashSet[Int] = mutable.HashSet[Int]()

  /**
    * Unique identifier counter
    */
  var UID: Int = 0L

  /**
    * Increases the unique id and returns the next unique id
    * @return The next id
    */
  private def nextId(): Int = {
    UID += 1
    UID
  }

  /**
    * Handle push message receipt logic
    * @param message The message
    * @param sender The sender
    */
  def handleLogic(message: Any, sender: ActorRef) = message match {
    case GenerateUniqueID() =>
      sender ! UniqueID(nextId())

    case AcknowledgeReceipt(id) =>
      if(receipt.contains(id)) {
        sender ! AcknowledgeReceipt(id)
      } else {
        sender ! NotAcknowledgeReceipt(id)
      }

    case Forget(id) =>
      if(receipt.contains(id)) {
        receipt.remove(id)
      }
      sender ! Forget(id)
  }

  /**
    * Adds the message id to the received set
    * @param id The message id
    */
  def updateFinished(id: Int): Unit ={
    require(id >= 0, s"id must be positive but got ${id}")
    receipt.add(id)
  }


}
