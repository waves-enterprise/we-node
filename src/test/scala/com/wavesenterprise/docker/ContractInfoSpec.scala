package com.wavesenterprise.docker

import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ContractInfoSpec extends PropSpec with ScalaCheckPropertyChecks with Matchers with ContractInfoGen {

  property("ContractInfo serialization roundtrip") {
    forAll(contractInfoGen) { info =>
      val recovered = ContractInfo.fromBytes(ContractInfo.toBytes(info))
      recovered shouldBe info
    }
  }
}
