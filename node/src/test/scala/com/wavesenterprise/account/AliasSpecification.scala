package com.wavesenterprise.account

import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class AliasSpecification extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  property("Correct alias should be valid") {
    forAll(validAliasStringGen) { s =>
      Alias.buildWithCurrentChainId(s) shouldBe 'right
    }
  }

  property("Incorrect alias should be invalid") {
    forAll(invalidAliasStringGen) { s =>
      Alias.buildWithCurrentChainId(s) shouldBe 'left
    }
  }
}
