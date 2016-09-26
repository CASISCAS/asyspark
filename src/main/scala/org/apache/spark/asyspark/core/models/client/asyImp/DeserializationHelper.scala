package org.apache.spark.asyspark.core.models.client.asyImp

import akka.actor.ActorSystem
import com.typesafe.config.Config

/** Singleton pattern
  * Created by wjf on 16-9-26.
  */
object DeserializationHelper {
  @volatile private var as: ActorSystem = null

  def getActorSystem(config: Config): ActorSystem = {
    if(as == null) {
      DeserializationHelper.synchronized {
        if (as == null) {
          as = ActorSystem("asysparkClient", config)
        }
      }
    }
    as
  }
}
