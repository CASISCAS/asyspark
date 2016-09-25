package org.apache.spark.asyspark.core.models.server

import org.apache.spark.asyspark.core.messages.server.request.{PullVector, PushVectorFloat}
import org.apache.spark.asyspark.core.messages.server.response.ResponseFloat
import org.apache.spark.asyspark.core.partitions.Partition

/**
  * Created by wjf on 16-9-25.
  */
private[asyspark] class PartialVectorFloat(partition: Partition) extends PartialVector[Float](partition) {

  override val data: Array[Float] = new Array[Float](size)

  override def receive: Receive = {
    case pull: PullVector => sender ! ResponseFloat(get(pull.keys))
    case push: PushVectorFloat =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
