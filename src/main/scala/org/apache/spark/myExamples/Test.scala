package org.apache.spark.myExamples

import java.io.{DataInputStream, File, FileInputStream, PrintWriter}
import java.util

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Seconds

import scala.collection.immutable.Vector
import scala.collection.mutable.Seq
import scala.concurrent.duration.Duration
import scala.util.Random
/**
  * Created by wjf on 16-8-29.
  */
object Test {
  val conf=new SparkConf().setMaster("local[1]").setAppName("test")
  val sc=new SparkContext(conf)



  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("test").master("local[*]").config("spark.scheduler.mode","FAIR").getOrCreate()
//    val data1 = sparkSession.read.textFile("hdfs://133.133.134.119:9000        /user/hadoop/data/wjf/Kmeans_random_small.txt").rdd.map {
//      line =>
//        val parts = line.split(" ")
//        (parts(0).toInt, parts(1).toInt)
//    }.cache()

    val rdd = sparkSession.sparkContext.parallelize((0 until 10000000).toSeq, 3)
    rdd.foreachPartition( x => println(x.count(_ => true)))
    //    val job1 = rdd.countAsync()
//    val rdd2 = sparkSession.sparkContext.parallelize(List(1000,2000,3000,4000,5000))
//    val job2 = rdd2.countAsync()
//    val res1 = ThreadUtils.awaitResult(job1, Duration.Inf)
//    println(res1)
//    val res2 = ThreadUtils.awaitResult(job2, Duration("20s"))
//    println(res2)



  }


//    println(data1.countAsync())
//////    val seq =(1 until 50000000).toSeq.map(x => x.toLong)
////
////    val data1 = sparkSession.sparkContext.parallelize(seq,4).cache()
//    println(data1.sortBy(x => x).max())


//    val data2 = sparkSession.sparkContext.parallelize{
//      Seq(for(0 until 1000){
//        (0 until 1000)
//      })
//    }

//    val array = new Array[Obj](20000)
//    for(i <- 0 until array.length){
//      val obj:Obj = Obj(new Array[Int](10 + i),10 + i)
//      for(j <- 0 until obj.len){
//        obj.array(j)= j
//      }
//      array(i)=obj
//    }
//
////    val seq=Array.ofDim[Int](1000,1000)
////    seq.foreach(x => x.foreach(print))
//    import scala.reflect.classTag
//    val data1 = sparkSession.sparkContext.parallelize(array)
//    println(data1.sortBy(x => x)(new Ordering[Obj]{
//      override def compare(x: Obj, y: Obj): Int = x.len.compareTo(y.len)
//    },classTag[Obj]).count())


//
//    val data1 = sc.parallelize(List((null,10), (null,1))).cache()
//    val data2 = sc.parallelize(List((null,1)), 1).cache()






// 小循环在外面比较高效
//    val k = 1000000
//    val array = new Array[Int](k)
//    val array2 = new Array[Int](k)
//    var start = System.nanoTime()
//    for(i <- 0 until 1000) {
//      for(j <- 0 until k) {
//        array(j) += i
//      }
//    }
//    var end = System.nanoTime()
//    println((end - start)/ 1e6)
//    start = System.nanoTime()
//    for(i <- 0 until k) {
//      for(j <- 0 until 1000){
//        array2(i) += j
//      }
//    }
//    end = System.nanoTime()
//    println((end -start)/ 1e6)

//
//    data1.join(data2).foreach(println)
//    println(data1.join(data2).partitioner.get.getPartition((1, (2, 10))))
//    println(data1.join(data2).partitioner.get.getPartition(None))
//    println(Int.MaxValue)
//    println(Int.MinValue)
//    val a= (1 << 31) +1
//    println(a)

//    data.takeOrdered(2).foreach(println)
//    val data2 = sc.parallelize(Seq())
//
//    println(data2.partitions.length)
//
//    val data3 = sc.makeRDD(Seq())
//    println(data3.partitions.length)
//








//   var data = sc.textFile("hdfs://133.133.134.119:9000/user/hadoop/data/wjf/Kmeans_random_small.txt").map{
//     line =>
//       val parts = line.split(" ")
//       (parts(0).toInt,parts(1).toInt)
//   }cache()
//    data.take(2).foreach(println)
//    println("id"+data.id)
//    for( i <- 0 until 100) {
//      for(j <- 0 until 10) {
//        data = data.filter(_._1 > i)
//
//        println(data.id)
//      }
//      println(data.count())
//
//    }


//    sc.stop()
//    data.foreach(println)
//    data.keys.foreach(println)
//    data.countByValue().foreach(println)
//    println("this is countByValue")
////    data.sortBy(x => x._1 * x._2).foreach(println)
//    data.intersection(data).foreach(println)
//    data.map(value => ( value,Unit)).foreach(println)



//    val array = Array(1,2,3)
//    array.scanLeft(0)(_ + _).foreach(println)

//    data.map(_._1).foreach(println)
//    data.foreach(println)
//
//    println("-----------------this is the sample-------------------")
//
//    data.sample(withReplacement = true , 3).foreach(println)

//    data.aggregateByKey(0)(math.max(_,_), math.max(_,_)).co
    //
    // llect().foreach(println)
//
//
//    val z=Array((1,1.0),(1,2.0),(1,3.0),(2,4.0),(2,5.0),(2,6.0))
//    val rdd= sc.parallelize(z,2)
//    val combine1= rdd.combineByKey(createCombiner = (v:Double) => (v:Double,1),
//      mergeValue = (c:(Double,Int),v:Double) =>(c._1 + v, c._2 +1),
//      mergeCombiners = (c1:(Double,Int),c2:(Double,Int)) => (c1._1 +c2._1, c1._2 + c1._2)
//
//    )
//    combine1.collect().foreach(println)
//
//
//    val list =Seq(1,3,2,4)
//    list.sorted(new Ordering[Int]{
//      override def compare(x: Int, y: Int): Int = y.compareTo(x)
//    }).foreach(println)

//   val spark =SparkSession.builder().appName("test").master("local").getOrCreate()
//
//    val data1 =spark.read.textFile("/home/hadoop/下载/Kmeans_random_big.txt").rdd.map{ line =>
//      val parts= line.split(" ")
//      (parts(0),parts(1))
//    }.cache()
//
//    val data2=spark.read.textFile("data/Kmeans_random_small.txt").rdd.map{ line =>
//      val parts= line.split(" ")
//      (parts(0),parts(1))
//    }.cache()
//
//    val rdd=spark.sparkContext.parallelize(Seq(1,2,3))
//    val rdd2= rdd.filter(_ > 2)
//    rdd2.foreach(println)
//
//
//
//
//
//    val data4= data1.mapValues( _ => 1.0)
//    val data5= data4.mapValues( _ => 2.0)
//    val data6= data5.mapValues(_ => 3.0)
//
//    val data7= data5.join(data6)
//    data1.cogroup(data2,data4 )
//
//    println("this is test")
//
////    val a =new java.lang.Double(3.732)
////    println( math.pow(a,80) )
////    printf("%.100f",math.pow(a,80))
//    val s= Seq(1,2,3)
//    println(s.exists(_ > 1))
//    testSort()




  def f(seq:Seq[Seq[Int]]): Unit ={
    println(seq.length)
  }

  def  writeFile(): Unit ={
    val random = new Random()
    val writer = new PrintWriter(new File("data/test.txt"))
    for(i <- 0 until 100000000) {
      val sb = new StringBuffer(random.nextInt(10000).toString + " ")
      for(i <- 0 until 2) {
        sb.append(i)

      }
      sb.append("\n")
      writer.write(sb.toString)
    }
    writer.close()
  }
  def testInputStream(): Unit ={

    for(i <- 0 until 80000){
      if(i % 100000 == 0){
        println(i)
      }
      val in  = new DataInputStream(new FileInputStream("data/mydata/people.json"))
      in.close()
    }
  }
  def testMap(): Unit ={

//    val data =sc.parallelize(Seq(1 to 10),2)
    val data=sc.makeRDD( 1 to 5,2)
    data.foreach(println)


    val rdd2= data.mapPartitions{ x=>

      val result =List[Int]()
      var i:Int =0
      while(x.hasNext){
        i = i +  x.next()
      }
      result.::(i).iterator
    }
    rdd2.foreach(println)

    val rdd3=data.mapPartitionsWithIndex {
      (x, iter) => {
        val result = List[Int]()
        var i = 0
        while (iter.hasNext) {
          i += iter.next()
        }
        result.::(x + "|"+i).iterator
      }
    }
    rdd3.foreach(println)


    val result =data.mapPartitions{ x =>
//        var seq :List[Int]=List.empty
         var seq:List[Int]=List[Int]()
        while(x.hasNext){
          val temp =x.next()
          seq= temp :: seq
        }
        seq.slice(0,2).iterator

    }
    println("element")
    result.collect().foreach(println)
    println(result.count())
  }


  def testSort(): Unit ={
    case class Node(var x:Seq[Int], var len:Long)
    val nodes=new Array[Node](100)
    for(i <- 0 until 100) {
      var v = Seq.empty[Int]
      for(j <- 0 until 100000) {
        v = v :+ j
      }
      nodes(i)=Node( v, i)
    }
    var start = System.nanoTime()
    nodes.sortBy(_.len)
    var end = System.nanoTime()
    println( (end - start) /1000)

    val array = new Array[Int](100)
    for(i <- 0 until 100) {
      array(i) = i
    }
    start = System.nanoTime()
    array.sorted
    end = System.nanoTime()
    println((end - start) /100)
  }
}
//
//class A{
//}
//object A{
//  def func: A = synchronized {
//    // some code here
//    A.synchronized{
//      //som code here
//    }
//  }
//}



