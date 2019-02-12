package org.antipathy.scoozie.builder

import org.scalatest.{FlatSpec, Matchers}

class MonadBuilderSpec extends FlatSpec with Matchers {

  behavior of "ObjectBuilder"

  it should "return the value of a function when the function completes" in {
    MonadBuilder.tryOperation { () =>
      "StringValue"
    } { _: String =>
      new RuntimeException("hmmm")
    } should be("StringValue")
  }

  it should "raise an error when a function exits abnormally" in {
    an[RuntimeException] should be thrownBy {
      MonadBuilder.tryOperation { () =>
        throw new RuntimeException("I did it on purpose")
      } { _: String =>
        new RuntimeException("hmmm")
      }
    }
  }

  it should "return the value of an optional when it contains one" in {
    MonadBuilder.getOrException { () =>
      Some("StringValue")
    } { () =>
      new RuntimeException("hmmm")
    } should be("StringValue")
  }

  it should "raise an error when an optional is empty" in {
    an[RuntimeException] should be thrownBy {
      MonadBuilder.getOrException { () =>
        None
      } { () =>
        new RuntimeException("hmmm")
      }
    }
  }
}
