/**
  *    Copyright (C) 2019 Antipathy.org <support@antipathy.org>
  *
  *    Licensed under the Apache License, Version 2.0 (the "License");
  *    you may not use this file except in compliance with the License.
  *    You may obtain a copy of the License at
  *
  *        http://www.apache.org/licenses/LICENSE-2.0
  *
  *    Unless required by applicable law or agreed to in writing, software
  *    distributed under the License is distributed on an "AS IS" BASIS,
  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *    See the License for the specific language governing permissions and
  *    limitations under the License.
  */
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
