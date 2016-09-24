package org.apache.spark.asyspark.core.partition

/**
  * Created by wjf on 16-9-24.
  */
trait Partitioner extends Serializable {
  /**
    * Assign a server to the given key
    * @param key The key to partition
    * @return The partition
    */
  def partition(key: Long): Partition

  /**
    * Returns all partitions
    * @return The array of partitions
    */
  def all(): Array[Partition]

}
