package org.apache.spark.asyspark.core.models.client

import scala.concurrent.{ExecutionContext, Future}

/**
  * A big vector supporting basic parameter server element-wise operations
  * {{{
  *   val vector:BigVector[Int] = ...
  *   vector.pull()
  *   vector.push()
  *   vector.destroy()
  * }}}
  * Created by wjf on 16-9-25.
  */
trait BigVector[V] extends Serializable {
  val size: Long


  /**
    * Pull a set of elements
    * @param keys The Indices of the elements
    * @param ec The implicit execution context in which  to execute the request
    * @return A Future containing the values of the elements at given rows, columns
    */
  //TODO it's convenient fo use scala.concurrent.ExecutionContext.Implicits.global
  // but we should do some optimization in production env
  def pull(keys: Array[Long])(implicit ec:ExecutionContext): Future[Array[V]]

  /**
    * Push a set of elements
    * @param keys The indices of the rows
    * @param values The values to update
    * @param ec  The Implicit execution context
    * @return A future
    */
  def push(keys: Array[Long], values: Array[V])(implicit ec: ExecutionContext): Future[Boolean]

  /**
    * Destroy the big vectors and its resources on the parameter server
    * @param ec The implicit execution context in which to execute the request
    * @return A future
    */
  def destroy()(implicit ec: ExecutionContext): Future[Boolean]
}
