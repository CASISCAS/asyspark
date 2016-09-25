package org.apache.spark.asyspark.core.partitions

/**
  * Created by wjf on 16-9-24.
  */
abstract class Partition(val index: Int)  extends Serializable {

  /**
    * check whether this partition contains some key
    * @param key the key
    * @return whether this partition contains some key
    */
  def contains(key: Long): Boolean

  /**
    * converts given global key to a continuous local array index
    * @param key the global key
    * @return the local index
    */
  def globalToLocal(key: Long): Int

  /**
    * the size of this partition
    * @return the size
    */
  def size:Int
}
