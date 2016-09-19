package org.apache.spark.mySql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
  * Created by wjf on 16-9-2.
  */
object TestBasic {
  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().master("local").appName("test sql basic").getOrCreate()
//    val context=spark.sqlContext
    import spark.implicits._
//    val df =spark.read.json("/usr/local/qq/fileReceived/facts.json")
//    df.show()
    val df=spark.read.json("data/mydata/people.json")
    df.printSchema()
    df.show()
    df.select("firstname").show()
    df.filter($"firstname" < "tom").show()
    df.select("firstname").collect().foreach(println)
  }

}
