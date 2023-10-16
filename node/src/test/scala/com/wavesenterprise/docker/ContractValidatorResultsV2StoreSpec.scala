package com.wavesenterprise.docker

import com.wavesenterprise.docker.validator.ContractValidatorResultsStore
import com.wavesenterprise.network.{ContractValidatorResultsGen, ContractValidatorResultsV2}
import com.wavesenterprise.state.ByteStr
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper._

class ContractValidatorResultsV2StoreSpec extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with ContractValidatorResultsGen {

  property("ContractValidatorV2Results equals is correct") {
    forAll(ContractValidatorResultsV2) { result =>
      result shouldBe result
      result should not be contractValidatorResultsV2Gen.generateSample()
    }
  }

  property("ContractValidatorResultsV2Store process one item correctly") {
    forAll(contractValidatorResultsV2Gen) { sample =>
      val store = new ContractValidatorResultsStore

      store.contains(sample) shouldBe false
      store.add(sample)
      store.contains(sample) shouldBe true
      store.findResults(sample.keyBlockId, sample.txId, Set(sample.sender.toAddress)) shouldBe Set(sample)
      store.removeExceptFor(ByteStr.empty)
      store.contains(sample) shouldBe false
    }
  }

  property("ContractValidatorResultsV2Store process multiple items correctly") {
    forAll(Gen.nonEmptyListOf(contractValidatorResultsV2Gen)) { samples =>
      val store = new ContractValidatorResultsStore

      samples.map(!store.contains(_)).forall(identity) shouldBe true
      samples.foreach(store.add)
      samples.map(store.contains).forall(identity) shouldBe true
      store.removeExceptFor(ByteStr.empty)
      samples.map(!store.contains(_)).forall(identity) shouldBe true
    }
  }
}
