package org.apache.spark.asyspark.core.models.server

import org.apache.spark.asyspark.core.messages.server.request.{PullVector, PushVectorDouble}
import org.apache.spark.asyspark.core.messages.server.response.ResponseDouble
import org.apache.spark.asyspark.core.partitions.Partition
import spire.implicits._
/**
  * Created by wjf on 16-9-25.
  */
private[asyspark] class PartialVectorDouble(partition: Partition) extends PartialVector[Double](partition) {
  override val data: Array[Double] = new Array[Double](size)
  override  def receive: Receive = {
    case pull: PullVector => sender ! ResponseDouble(get(pull.keys))
    case push: PushVectorDouble =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
