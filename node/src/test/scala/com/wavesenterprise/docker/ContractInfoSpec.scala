package com.wavesenterprise.docker

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class ContractInfoSpec extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with ContractInfoGen {

  property("ContractInfo serialization roundtrip") {
    forAll(contractInfoGen) { info =>
      val recovered = ContractInfo.fromBytes(ContractInfo.toBytes(info))
      recovered shouldBe info
    }
  }
}
