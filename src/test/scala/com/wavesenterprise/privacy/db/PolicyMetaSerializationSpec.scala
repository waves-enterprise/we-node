package com.wavesenterprise.privacy.db

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.privacy.PolicyMetaData
import org.scalatest.{FreeSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PolicyMetaSerializationSpec extends FreeSpec with Matchers with ScalaCheckPropertyChecks with TransactionGen {

  "Byte serialization " in forAll(policyMetaData) { pmd =>
    pmd shouldBe PolicyMetaData.fromBytes(pmd.bytes()).get
  }

}
