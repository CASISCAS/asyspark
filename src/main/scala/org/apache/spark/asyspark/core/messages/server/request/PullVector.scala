package org.apache.spark.asyspark.core.messages.server.request

/**
  * Created by wjf on 16-9-23.
  */
private[asyspark] case class PullVector(keys: Array[Long]) extends Request
