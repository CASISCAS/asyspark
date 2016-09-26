package org.apache.spark.examples

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.asyspark.core.Client
import org.apache.spark.asyspark.core.Main._

/**
  * Created by wjf on 16-9-26.
  */
object TestClient {
  def main(args: Array[String]): Unit = {

    val default = ConfigFactory.parseResourcesAnySyntax("asyspark")
    val config = ConfigFactory.parseFile(new File(getClass.getClassLoader.getResource("asyspark.conf").getFile)).withFallback(default).resolve()
    val client = Client(config)
    val vector  = client.bigVector[Long](2)
    println(vector.size)

  }

}
