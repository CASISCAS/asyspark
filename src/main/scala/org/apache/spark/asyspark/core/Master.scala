package org.apache.spark.asyspark.core

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Props, Terminated}
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.asyspark.core.messages.master.{ClientList, RegisterClient, RegisterServer, ServerList}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by wjf on 16-9-22.
  * the master that registers the spark workers
  */
class Master() extends Actor with ActorLogging {

  /**
    * collection of servers available
    */
  var servers = Set.empty[ActorRef]

  /**
    * collection of clients
    */
  var clients = Set.empty[ActorRef]

  override def receive: Receive = {
    case RegisterServer(server) =>
      log.info(s"Registering server ${server.path.toString}")
      println("register server")
      servers += server
      context.watch(server)
      sender ! true

    case RegisterClient(client)  =>
      log.info(s"Registering client ${sender.path.toString}")
      clients += client
      context.watch(client)
      sender ! true

    case ServerList() =>
      log.info(s"Sending current server list to ${sender.path.toString}")
      sender ! servers.toArray

    case ClientList() =>
      log.info(s"Sending current client list to ${sender.path.toString}")
      sender ! clients.toArray


    case Terminated(actor) =>
      actor match {
        case server: ActorRef if servers contains server =>
          log.info(s"Removing server ${server.path.toString}")
          servers -= server
        case client: ActorRef if clients contains client =>
          log.info(s"Removing client ${client.path.toString}")
          clients -= client
        case actor: ActorRef =>
          log.warning(s"Actor ${actor.path.toString} will be terminated for some unknown reason")
      }
  }

}

object Master extends StrictLogging {
  def run(config: Config): Future[(ActorSystem, ActorRef)] = {
    logger.debug("Starting master actor system")
    val system = ActorSystem(config.getString("asyspark.master.system"), config.getConfig("asyspark.master"))
    logger.debug("Starting master")
    val master = system.actorOf(Props[Master], config.getString("asyspark.master.name"))
    implicit val timeout = Timeout(config.getDuration("asyspark.master.startup-timeout", TimeUnit.MILLISECONDS) milliseconds)
    implicit val ec = ExecutionContext.Implicits.global
    val address = Address("akka.tcp", config.getString("asyspark.master.system"), config.getString("asyspark.master.host"),
    config.getString("asyspark.master.port").toInt)
    system.actorSelection(master.path.toSerializationFormat).resolveOne().map {
      case actor: ActorRef =>
        logger.debug("Master successfully started")
        (system, master)

    }
  }

}
