package org.apache.spark.asyspark.core.messages.server.request

/**
  * Created by wjf on 16-9-23.
  */
private[asyspark] case class PushVectorLong(id: Int, keys: Array[Long], values:Array[Long]) extends Request

