package org.antipathy.scoozie.functions

import org.scalatest.{FlatSpec, Matchers}

class CoordinatorFunctionsSpec extends FlatSpec with Matchers {

  behavior of "CoordinatorFunctions"

  it should "provide string representations of coordinator functions" in {

    CoordinatorFunctions.minutes(1) should be("${coord:minutes(1)}")
    CoordinatorFunctions.hours(1) should be("${coord:hours(1)}")
    CoordinatorFunctions.days(1) should be("${coord:days(1)}")
    CoordinatorFunctions.endOfDays(1) should be("${coord:coord:endOfDays(1)}")
    CoordinatorFunctions.months(1) should be("${coord:months(1)}")
    CoordinatorFunctions.endOfMonths(1) should be("${coord:endOfMonths(1)}")
    CoordinatorFunctions.cron("* * * * * *") should be("${* * * * * *}")
  }
}
