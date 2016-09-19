package org.apache.spark.myExamples

/**
  * Created by wjf on 16-9-18.
  */
import org.apache.spark.sql._

object Env {
  val spark = SparkSession.builder.master("local").getOrCreate()
}

import Env.spark.implicits._

abstract class A[T : Encoder] {}

object TestEncoder extends A[String] {
  def func(str:String):String = str
  def main(args: Array[String]): Unit = {
    Env.spark.createDataset(Seq("a","b","c")).map(func).show()
  }
}