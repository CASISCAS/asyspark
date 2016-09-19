package org.apache.spark.myExamples

import org.apache.spark.util.Utils
import scala.collection.JavaConverters._

/**
  * Created by wjf on 16-8-31.
  */
object TestDriverSubmission {

  def main(args: Array[String]): Unit = {

    if(args.length <1){
      println("Usage: TestDriverSubmission <Seconds to sleep>")
      System.exit(0)
    }

    val secondsToSleep=args(0).toInt

    val env=System.getenv()
    val properties =Utils.getSystemProperties
    println("Environment variable containing SPARK_TEST")
    env.asScala.filter{case (key,_) => key.contains("SPARK_TEST")}.foreach(println)
    println("System properties contain spark_test")
    properties.filter{ case(key,_) => key.contains("spark_test")}.foreach(println)


    for(i<- 1 until secondsToSleep){
      println(s"Alive $i out of $secondsToSleep seconds")
      Thread.sleep(1000)
    }

  }

}
