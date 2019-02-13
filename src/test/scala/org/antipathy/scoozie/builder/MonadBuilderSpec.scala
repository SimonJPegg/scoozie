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
