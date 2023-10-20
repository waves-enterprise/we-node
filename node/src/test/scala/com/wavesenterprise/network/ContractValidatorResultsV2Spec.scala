package com.wavesenterprise.network

import com.wavesenterprise.transaction.WithSenderAndRecipient
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ContractValidatorResultsV2Spec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with Eventually with WithSenderAndRecipient
    with ContractValidatorResultsGen {

  "ContractValidatorResultsV2Spec" - {
    import com.wavesenterprise.network.message.MessageSpec.ContractValidatorResultsV2Spec._

    "deserializeData(serializedData(data)) == data" in forAll(contractValidatorResultsV2Gen) { message =>
      val restoredMessage = deserializeData(serializeData(message)).get
      restoredMessage shouldBe message
      restoredMessage.signatureValid() shouldBe true
    }
  }
}
