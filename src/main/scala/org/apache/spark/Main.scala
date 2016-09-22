package org.apache.spark

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.concurrent.ExecutionContext

/**
  * This is the main class that runs when you start asyspark. By manually specifying additional command-line options it is
  * possible to start a master node or a parameter server.
  *
  * To start a master node:
  * {{{
  *   java -jar /path/to/compiled/asyspark.jar master -c /path/to/asyspark.conf
  * }}}
  *
  * To start a parameter server node:
  * {{{
  *   java -jar /path/to/compiled/asyspark.jar server -c /path/to/asyspark.conf
  * }}}
  *
  * Alternatively you can use the scripts provided in the ./sbin/ folder of the project to automatically construct a
  * master and servers over passwordless ssh.
  */
object Main extends StrictLogging {

  /**
    * Main entry point of the application
    *
    * @param args The command-line arguments
    */
  def main(args: Array[String]): Unit = {
    // todo currently we don't support command line mode

    val parser = new scopt.OptionParser[Options]("asyspark") {
      head("asyspark", "0.1")
      opt[File]('c', "config") valueName "<file>" action { (x, c) =>
        c.copy(config = x)
      } text "The .conf file for asyspark"
      cmd("master") action { (_, c) =>
        c.copy(mode = "master")
      } text "Starts a master node."
      cmd("server") action { (_, c) =>
        c.copy(mode = "server")
      } text "Starts a server node."
    }
    val arg =Array[String]("server")
    parser.parse(arg, Options()) match {
      case Some(options) =>

        // Read configuration
        logger.debug("Parsing configuration file")
        val default = ConfigFactory.parseResourcesAnySyntax("asyspark")
        val config = ConfigFactory.parseFile(options.config).withFallback(default).resolve()


        // Start specified mode of operation
        implicit val ec = ExecutionContext.Implicits.global
        options.mode match {
          case "server" => Server.run(config).onSuccess {
            case (system, ref) => sys.addShutdownHook {
              logger.info("Shutting down")
              system.shutdown()
              system.awaitTermination()
            }
          }
          case "master" => Master.run(config).onSuccess {
            case (system, ref) => sys.addShutdownHook {
              logger.info("Shutting down")
              system.shutdown()
              system.awaitTermination()
            }
          }
          case _ =>
            parser.showUsageAsError
            System.exit(1)
        }
      case None => System.exit(1)
    }

  }

  /**
    * Command-line options
    *
    * @param mode The mode of operation (either "master" or "server")
    * @param config The configuration file to load (defaults to the included asyspark.conf)
    * @param host The host of the parameter server (only when mode of operation is "server")
    * @param port The port of the parameter server (only when mode of operation is "server")
    */
  private case class Options(mode: String = "",
                             config: File = new File(getClass.getClassLoader.getResource("asyspark.conf").getFile),
                             host: String = "localhost",
                             port: Int = 0)

}