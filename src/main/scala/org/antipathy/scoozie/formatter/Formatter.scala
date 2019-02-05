// $COVERAGE-OFF$
package org.antipathy.scoozie.formatter

/**
  * Base trait for formatting
  * @tparam T The type of object to format
  */
trait Formatter[T] {

  /**
    * Method for formatting XML nodes
    *
    * @param t the item to format
    * @return document in string format
    */
  def format(t: T): String
}
// $COVERAGE-ON$
