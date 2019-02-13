// $COVERAGE-OFF$
package org.antipathy.scoozie.properties

trait PropertyFormatter {

  /**
    * expected predicate pattern for oozie switches
    */
  private val Pattern = """[${].*[}]""".r

  /**
    * format predicates to expected pattern
    */
  protected def formatProperty(property: String): String = property match {
    case Pattern() => property
    case _         => "${" + property + "}"
  }
}
// $COVERAGE-ON$
