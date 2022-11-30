package com.wavesenterprise.docker

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.docker.validator.ContractValidatorResultsStore
import com.wavesenterprise.network.ContractValidatorResults
import com.wavesenterprise.state.ByteStr
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class ContractValidatorResultsStoreSpec extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {

  val resultsGen: Gen[ContractValidatorResults] = for {
    account     <- accountGen
    txId        <- bytes32gen
    keyBlockId  <- bytes32gen
    resultsHash <- bytes32gen
  } yield ContractValidatorResults(account, ByteStr(txId), ByteStr(keyBlockId), ByteStr(resultsHash))

  property("ContractValidatorResults equals is correct") {
    forAll(ContractValidatorResults) { result =>
      result shouldBe result
      result should not be resultsGen.generateSample()
    }
  }

  property("ContractValidatorResultsStore process one item correctly") {
    forAll(resultsGen) { sample =>
      val store = new ContractValidatorResultsStore

      store.contains(sample) shouldBe false
      store.add(sample)
      store.contains(sample) shouldBe true
      store.findResults(sample.keyBlockId, sample.txId, Set(sample.sender.toAddress)) shouldBe Set(sample)
      store.removeExceptFor(ByteStr.empty)
      store.contains(sample) shouldBe false
    }
  }

  property("ContractValidatorResultsStore process multiple items correctly") {
    forAll(Gen.nonEmptyListOf(resultsGen)) { samples =>
      val store = new ContractValidatorResultsStore

      samples.map(!store.contains(_)).forall(identity) shouldBe true
      samples.foreach(store.add)
      samples.map(store.contains).forall(identity) shouldBe true
      store.removeExceptFor(ByteStr.empty)
      samples.map(!store.contains(_)).forall(identity) shouldBe true
    }
  }
}
