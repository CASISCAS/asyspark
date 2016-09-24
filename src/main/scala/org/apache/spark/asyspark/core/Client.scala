package org.apache.spark.asyspark.core

import java.util.concurrent.TimeUnit
import javassist.bytecode.stackmap.TypeTag

import akka.pattern.ask
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.remote.ContainerFormats.ActorRef
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.spark.asyspark.core.messages.master.RegisterClient
import org.apache.spark.asyspark.core.partition.Partitioner
import org.apache.spark.asyspark.core.partition.range.RangePartitioner

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
/**
  * The client is to spawn a large distributed model such as BigVector on parameter servers
  * Created by wjf on 16-9-24.
  */
class Client(val config: Config, private[asyspark] val system: ActorSystem,
             private[asyspark] val master: ActorRef) {
  private implicit val timeout = Timeout(config.getDuration("asyspark.client.timeout", TimeUnit.MILLISECONDS) milliseconds)
  private implicit val ec = ExecutionContext.Implicits.global
  private[asyspark] val actor = system.actorOf(Props[ClientActor])
  private[asyspark] val registration = master ? RegisterClient(actor)


  def bigVector[V: breeze.math.Semiring: TypeTag](keys: Long, modelsPerServer: Int = 1): BigVector[V] = {
    bigVector[V](keys, modelsPerServer, (partitions: Int, keys: Long) => RangePartitioner(partitions, keys))
  }
  def bigVector[V: breeze.math.Semiring: TypeTag](keys: Long, modelsPerServer: Int = 1,
                                                  createPartitioner: (Int, Long) => Partitioner): BigVector[V] = {

  }

}
private class ClientActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case x => log.info(s"Client actor reveiced message ${x}")
  }
}