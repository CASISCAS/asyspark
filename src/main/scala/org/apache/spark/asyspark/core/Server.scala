package org.apache.spark.asyspark.core

/**
  * Created by wjf on 16-9-22.
  */

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.asyspark.core.messages.master.RegisterServer

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * A parameter server
  */
private class Server extends Actor with ActorLogging {

  override def receive: Receive = {
    case x =>
      log.warning(s"Received unknown message of type ${x.getClass}")
  }
}
/**
  * The parameter server object
  */
private object Server extends StrictLogging {

  /**
    * Starts a parameter server ready to receive commands
    *
    * @param config The configuration
    * @return A future containing the started actor system and reference to the server actor
    */
  def run(config: Config): Future[(ActorSystem, ActorRef)] = {

    logger.debug(s"Starting actor system ${config.getString("asyspark.server.system")}")
    val system = ActorSystem(config.getString("asyspark.server.system"), config.getConfig("asyspark.server"))

    logger.debug("Starting server actor")
    val server = system.actorOf(Props[Server], config.getString("asyspark.server.name"))

    logger.debug("Reading master information from config")
    val masterHost = config.getString("asyspark.master.host")
    val masterPort = config.getInt("asyspark.master.port")
    val masterName = config.getString("asyspark.master.name")
    val masterSystem = config.getString("asyspark.master.system")

    logger.info(s"Registering with master ${masterSystem}@${masterHost}:${masterPort}/user/${masterName}")
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = Timeout(config.getDuration("asyspark.server.registration-timeout", TimeUnit.MILLISECONDS) milliseconds)
    val master = system.actorSelection(s"akka.tcp://${masterSystem}@${masterHost}:${masterPort}/user/${masterName}")
    val registration = master ? RegisterServer(server)

    registration.map {
      case a =>
        logger.info("Server successfully registered with master")
        (system, server)
    }

  }
}

