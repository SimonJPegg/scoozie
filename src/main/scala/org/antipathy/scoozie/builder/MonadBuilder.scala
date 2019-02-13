package org.antipathy.scoozie.builder

import scala.util.{Failure, Success, Try}

/**
  * Class for accessing values contained in Monads
  */
private[scoozie] object MonadBuilder {

  /**
    * Attempt the passed in operation and raise an error if failure occurs
    *
    * @param operation the operation to attempt
    * @param errorFunction the error function
    * @tparam T the Type of object returned
    * @return the result of a successful operation
    */
  def tryOperation[T](operation: () => T)(errorFunction: Throwable => Throwable): T =
    Try {
      operation()
    } match {
      case Success(value)     => value
      case Failure(exception) => throw errorFunction(exception)
    }

  /**
    * Attempt the passed in operation and return the value of the optional result
    * @param operation the operation to try
    * @param errorFunction the error to raise when `None` is returned
    * @tparam T The type of the returned object
    * @return the value of the optional type
    */
  def valueOrException[T](operation: () => Option[T])(errorFunction: () => RuntimeException): T =
    operation() match {
      case Some(value) => value
      case None        => throw errorFunction()
    }
}
