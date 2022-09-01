package com.wavesenterprise.network

import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.WithSenderAndRecipient
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ContractValidatorResultsSpec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with Eventually with WithSenderAndRecipient {

  private val networkMessageGen: Gen[ContractValidatorResults] = for {
    sender      <- accountGen
    txId        <- bytes32gen.map(ByteStr(_))
    keyBlockId  <- bytes64gen.map(ByteStr(_))
    resultsHash <- bytes32gen.map(ByteStr(_))
  } yield ContractValidatorResults(sender, txId, keyBlockId, resultsHash)

  "ContractValidatorResultsSpec" - {
    import com.wavesenterprise.network.message.MessageSpec.ContractValidatorResultsSpec._

    "deserializeData(serializedData(data)) == data" in forAll(networkMessageGen) { message =>
      val restoredMessage = deserializeData(serializeData(message)).get
      restoredMessage shouldBe message
      restoredMessage.signatureValid() shouldBe true
    }
  }
}
