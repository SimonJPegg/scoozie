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
