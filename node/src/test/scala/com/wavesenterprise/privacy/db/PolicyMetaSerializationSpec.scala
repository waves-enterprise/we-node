package com.wavesenterprise.privacy.db

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.privacy.PolicyMetaData
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PolicyMetaSerializationSpec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with TransactionGen {

  "Byte serialization " in forAll(policyMetaData) { pmd =>
    pmd shouldBe PolicyMetaData.fromBytes(pmd.bytes()).get
  }

}
