package org.apache.spark.examples

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.asyspark.core.Main.{logger => _, _}
import org.apache.spark.asyspark.core.messages.master.ServerList

/**
  * Created by wjf on 16-9-27.
  */
object TestRemote extends StrictLogging {
  def main(args: Array[String]): Unit = {

    val default = ConfigFactory.parseResourcesAnySyntax("asyspark")
    val config = ConfigFactory.parseFile(new File(getClass.getClassLoader.getResource("asyspark.conf").getFile)).withFallback(default).resolve()
    val system = ActorSystem(config.getString("asyspark.server.system"), config.getConfig("asyspark.server"))

    val serverHost = config.getString("asyspark.master.host")
    val serverPort = config.getInt("asyspark.master.port")
    val serverName = config.getString("asyspark.master.name")
    val serverSystem = config.getString("asyspark.master.system")
    logger.debug("Starting server actor")

    val master = system.actorSelection(s"akka.tcp://${serverSystem}@${serverHost}:${serverPort}/user/${serverName}")


    println(master.anchorPath.address)

  }

}
