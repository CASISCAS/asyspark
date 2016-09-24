package org.apache.spark.asyspark.core.partition.range

import org.apache.spark.asyspark.core.partition.{Partition, Partitioner}

/**
  * Created by wjf on 16-9-24.
  */
class RangePartitioner(val partitions: Array[Partition], val numOfSmallPartitions: Int, val keysSmallPartiiton: Int, val keysSize: Long) extends Partitioner {
  val numAllSamllPartitions: Long = numOfSmallPartitions.toLong * keysSmallPartiiton.toLong
  val sizeOflargePartitions = keysSmallPartiiton + 1

  @inline
  override def partition(key: Long): Partition = {
    require(key >= 0 && key <= keysSize, s"IndexOutOfBoundsException ${key} while size = ${keysSize}")
    val index = if(key < numOfSmallPartitions) {
      (key / numOfSmallPartitions).toInt
    } else {
      (numOfSmallPartitions + (keysSize - numAllSamllPartitions) / sizeOflargePartitions ).toInt
    }
    partitions(index)
  }

  override def all(): Array[Partition] = partitions

}
object RangePartitioner {

  /**
    * Create a RangePartitioner for given number of partition and keys
    * @param numOfPartitions The number of partiitons
    * @param numberOfKeys The number of keys
    * @return A rangePartitioner
    */
  def apply(numOfPartitions: Int, numberOfKeys: Long): RangePartitioner = {
    val partitions = new Array[Partition](numOfPartitions)
    // this largePartition just has more than one element then smalllPartition
    val numLargePartition = numberOfKeys % numOfPartitions
    val numSmallPartition = numOfPartitions - numLargePartition
    val keysSmallPartition = ((numberOfKeys - numLargePartition) / numOfPartitions).toInt
    var i = 0
    var start: Long = 0L
    var end: Long = 0L
    while(i < numOfPartitions) {
      if(i < numSmallPartition) {
        end += keysSmallPartition
        partitions(i) = new RangePartition(i, start, end)
        start += keysSmallPartition
      } else {
        end += keysSmallPartition + 1
        partitions(i) = new RangePartition(i, start, end)
        start += keysSmallPartition + 1
      }
      i += 1
    }


    new RangePartitioner(partitions, numOfPartitions, keysSmallPartition, numberOfKeys)

  }
}
