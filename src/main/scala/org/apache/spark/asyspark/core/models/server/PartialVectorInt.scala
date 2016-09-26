package org.apache.spark.asyspark.core.models.server

import org.apache.spark.asyspark.core.messages.server.request.{PullVector, PushVectorInt}
import org.apache.spark.asyspark.core.messages.server.response.ResponseInt
import org.apache.spark.asyspark.core.partitions.Partition
import spire.implicits._

/**
  * Created by wjf on 16-9-25.
  */
private[asyspark] class PartialVectorInt(partition: Partition) extends PartialVector[Int](partition) {

  override val data: Array[Int] = new Array[Int](size)

  override def receive: Receive = {
    case pull: PullVector => sender ! ResponseInt(get(pull.keys))
    case push: PushVectorInt =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
