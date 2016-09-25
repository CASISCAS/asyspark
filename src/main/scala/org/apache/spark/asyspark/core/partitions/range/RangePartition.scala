package org.apache.spark.asyspark.core.partitions.range

import org.apache.spark.asyspark.core.partitions.Partition

/**
  * A range partition
  * Created by wjf on 16-9-24.
  */
class RangePartition(index: Int, val start: Long, val end: Long) extends Partition(index) {

  @inline
  override def contains(key: Long): Boolean = key >= start && key <= end

  @inline
  override def size: Int = (end - start).toInt

  /**
    * Converts given key to a continuous local array index[0,1,2...]
    * @param key the global key
    * @return the local index
    */
  @inline
  def globalToLocal(key: Long): Int = (key - start).toInt

}
