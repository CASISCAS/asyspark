package org.apache.spark.asyspark.core.models.server

import akka.actor.{Actor, ActorLogging}
import spire.algebra.Semiring
import spire.implicits._
import org.apache.spark.asyspark.core.partitions.Partition
import scala.reflect.ClassTag

/**
  * Created by wjf on 16-9-25.
  */
private[asyspark] abstract class PartialVector[@specialized V:Semiring : ClassTag](partition: Partition) extends Actor
  with ActorLogging with PushLogic {

  /**
    * the size of the partial vector
    */
  val size: Int = partition.size

  /**
    * the data array contains the elements
    */
  val data: Array[V]

  /**
    * update the data of the partial model by aggregating given keys and values into it
    * @param keys The keys
    * @param values The values
    * @return
    */
  //todo I thinks this imp can be optimized
  def update(keys: Array[Long], values: Array[V]): Boolean = {
    var i = 0
    try {
      while (i < keys.length) {
        val key = partition.globalToLocal(keys(i))
        // this is imp with the help of spire.implicits._
        data(key) += values(i)
        i += 1
      }
      true
    } catch {
      case Exception => false
    }
  }

  def get(keys: Array[Long]): Array[V] = {
    var i =0
    val a = new Array[V](keys.length)
    while(i < keys.length) {
      val key = partition.globalToLocal(keys(i))
      a(i)  = data(key)
      i += 1
    }
    a
  }

  log.info(s"Constructed PartialVector[${implicitly[ClassTag[V]]}] of size $size (partition id: ${partition.index})")

}
