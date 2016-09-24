package org.apache.spark.asyspark.core.messages.master

import akka.actor.ActorRef

/**
  * Created by wjf on 16-9-24.
  */
private[asyspark] case class RegisterClient(client: ActorRef)
