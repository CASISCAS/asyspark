package org.apache.spark.asyspark.core.models.server

import org.apache.spark.asyspark.core.messages.server.request.{PullVector, PushVectorLong}
import org.apache.spark.asyspark.core.messages.server.response.ResponseLong
import org.apache.spark.asyspark.core.partitions.Partition

/**
  * Created by wjf on 16-9-25.
  */
private[asyspark] class PartialVectorLong(partition: Partition) extends PartialVector[Long](partition) {

  override val data: Array[Long] = new Array[Long](size)

  override def receive: Receive = {
    case pull: PullVector => sender ! ResponseLong(get(pull.keys))
    case push: PushVectorLong =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
