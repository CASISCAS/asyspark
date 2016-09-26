package org.apache.spark.asyspark.core.models.client.asyImp

import org.apache.spark.asyspark.core.partitions.Partitioner
import akka.actor.ActorRef
import com.typesafe.config.Config
import org.apache.spark.asyspark.core.messages.server.request.PushVectorLong
import org.apache.spark.asyspark.core.messages.server.response.ResponseLong
/**
  * Created by wjf on 16-9-26.
  */
private[asyspark] class AsyBigVectorLong(partitioner: Partitioner,
                                         models: Array[ActorRef],
                                         config: Config,
                                         keys: Long)
  extends AsyBigVector[Long, ResponseLong, PushVectorLong](partitioner, models, config, keys) {
  /**
    * Creates a push message from given sequence of keys and values
    *
    * @param id The identifier
    * @param keys The keys
    * @param values The values
    * @return A PushVectorLong message for type V
    */
  @inline
  override protected def toPushMessage(id: Int, keys: Array[Long], values: Array[Long]): PushVectorLong = {
    PushVectorLong(id, keys, values)
  }

  /**
    * Extracts a value from a given response at given index
    *
    * @param response The response
    * @param index The index
    * @return The value
    */
  @inline
  override protected def toValue(response: ResponseLong, index: Int): Long = response.values(index)

}
