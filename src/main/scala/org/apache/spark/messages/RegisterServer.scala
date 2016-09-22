package org.apache.spark.messages

import akka.actor.ActorRef

/**
  * Created by wjf on 16-9-22.
  */
case class RegisterServer(server: ActorRef)
