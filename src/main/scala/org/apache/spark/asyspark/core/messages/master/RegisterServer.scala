package org.apache.spark.asyspark.core.messages.master

import akka.actor.ActorRef

/**
  * Created by wjf on 16-9-22.
  */
private[asyspark] case class RegisterServer(server: ActorRef)
