package com.wavesenterprise.docker

import com.wavesenterprise.docker.validator.ContractValidatorResultsStore
import com.wavesenterprise.network.{ContractValidatorResultsGen, ContractValidatorResultsV1}
import com.wavesenterprise.state.ByteStr
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class ContractValidatorResultsV1StoreSpec extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with ContractValidatorResultsGen {

  property("ContractValidatorV1Results equals is correct") {
    forAll(ContractValidatorResultsV1) { result =>
      result shouldBe result
      result should not be contractValidatorResultsV1Gen.generateSample()
    }
  }

  property("ContractValidatorResultsV1Store process one item correctly") {
    forAll(contractValidatorResultsV1Gen) { sample =>
      val store = new ContractValidatorResultsStore

      store.contains(sample) shouldBe false
      store.add(sample)
      store.contains(sample) shouldBe true
      store.findResults(sample.keyBlockId, sample.txId, Set(sample.sender.toAddress)) shouldBe Set(sample)
      store.removeExceptFor(ByteStr.empty)
      store.contains(sample) shouldBe false
    }
  }

  property("ContractValidatorResultsV1Store process multiple items correctly") {
    forAll(Gen.nonEmptyListOf(contractValidatorResultsV1Gen)) { samples =>
      val store = new ContractValidatorResultsStore

      samples.map(!store.contains(_)).forall(identity) shouldBe true
      samples.foreach(store.add)
      samples.map(store.contains).forall(identity) shouldBe true
      store.removeExceptFor(ByteStr.empty)
      samples.map(!store.contains(_)).forall(identity) shouldBe true
    }
  }
}
