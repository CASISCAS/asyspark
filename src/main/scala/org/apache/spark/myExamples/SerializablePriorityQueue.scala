package org.apache.spark.myExamples
import java.util.{PriorityQueue =>JPriorityQueue}
/**
  * Created by wjf on 16-8-29.
  */
class SerializablePriorityQueue[A](maxSize:Int)(implicit ord:Ordering[A]) extends Serializable {
  private val pq=new JPriorityQueue[A](maxSize,ord)
  def getPQ:JPriorityQueue[A]={
    pq
  }
  def +=(elem: A): Boolean = {

      pq.add(elem)


  }
}
