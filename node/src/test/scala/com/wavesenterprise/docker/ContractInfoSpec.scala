package com.wavesenterprise.docker

import com.wavesenterprise.transaction.docker.ContractTransactionGen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class ContractInfoSpec extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with ContractInfoGen with ContractTransactionGen {

  property("ContractInfo serialization roundtrip") {
    forAll(dockerContractInfoGen) { info =>
      val recovered = ContractInfo.fromBytes(ContractInfo.toBytes(info))
      recovered shouldBe info
    }
  }

  property("UpdateContractTransaction with version under 5 should not erase groupParticipants and groupOwners") {

    val (address, updateContractTransactionV4, contractInfo) = (for {
      address                     <- addressGen
      updateContractTransactionV4 <- updateContractV4ParamGen()
      contractInfo                <- contractInfoGen()
    } yield (address, updateContractTransactionV4, contractInfo)).sample.get

    ContractInfo.apply(updateContractTransactionV4, contractInfo.copy(isConfidential = true, groupOwners = Set(address))).groupOwners shouldBe Set(
      address)
  }

}
