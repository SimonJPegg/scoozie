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

import org.scalatest.{FlatSpec, Matchers}

class MonadBuilderSpec extends FlatSpec with Matchers {

  behavior of "ObjectBuilder"

  it should "return the value of a function when the function completes" in {
    MonadBuilder.tryOperation { () =>
      "StringValue"
    } { e: Throwable =>
      new RuntimeException("hmmm", e)
    } should be("StringValue")
  }

  it should "raise an error when a function exits abnormally" in {
    an[RuntimeException] should be thrownBy {
      MonadBuilder.tryOperation { () =>
        throw new RuntimeException("I did it on purpose")
      } { e: Throwable =>
        new RuntimeException("hmmm", e)
      }
    }
  }

  it should "return the value of an optional when it contains one" in {
    MonadBuilder.valueOrException { () =>
      Some("StringValue")
    } { () =>
      new RuntimeException("hmmm")
    } should be("StringValue")
  }

  it should "raise an error when an optional is empty" in {
    an[RuntimeException] should be thrownBy {
      MonadBuilder.valueOrException { () =>
        None
      } { () =>
        new RuntimeException("hmmm")
      }
    }
  }
}
