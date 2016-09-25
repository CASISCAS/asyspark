package org.apache.spark.asyspark.core

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, _}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.spark.asyspark.core.messages.master.RegisterClient
import org.apache.spark.asyspark.core.models.client.BigVector
import org.apache.spark.asyspark.core.partitions.{Partition, Partitioner}
import org.apache.spark.asyspark.core.partitions.range.RangePartitioner

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.runtime.universe.{TypeTag, typeOf}
/**
  * The client is to spawn a large distributed model such as BigVector on parameter servers
  * Created by wjf on 16-9-24.
  */
class Client(val config: Config, private[asyspark] val system: ActorSystem,
             private[asyspark] val master: ActorRef) {
  private implicit val timeout = Timeout(config.getDuration("asyspark.client.timeout", TimeUnit.MILLISECONDS) milliseconds)
  private implicit val ec = ExecutionContext.Implicits.global
  private[asyspark] val actor = system.actorOf(Props[ClientActor])
  // use ask to get a reply
  private[asyspark] val registration = master ? RegisterClient(actor)


  def bigVector[V: breeze.math.Semiring: TypeTag](keys: Long, modelsPerServer: Int = 1): BigVector[V] = {
    bigVector[V](keys, modelsPerServer, (partitions: Int, keys: Long) => RangePartitioner(partitions, keys))
  }
  def bigVector[V: breeze.math.Semiring: TypeTag](keys: Long, modelsPerServer: Int = 1,
                                                  createPartitioner: (Int, Long) => Partitioner): BigVector[V] = {

    val propFunction = numberType[V] match {
      case "Int" => (partition: Partition) => Props(classOf[PartialVectorInt], partition)

    }

  }

  def numberType[V: TypeTag]: String = {
    implicitly[TypeTag[V]].tpe match {
      case x if x <:< typeOf[Int] => "Int"
      case x if x <:< typeOf[Long] => "Long"
      case x if x <:< typeOf[Float] => "Float"
      case x if x <:< typeOf[Double] => "Double"
      case x => s"${x.toString}"
    }
  }

  /**
    * Stops the client
    */
  def stop(): Unit ={
    system.terminate()
  }

}
private class ClientActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case x => log.info(s"Client actor reveiced message ${x}")
  }
}