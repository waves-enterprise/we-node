package com.wavesenterprise.network

import com.wavesenterprise.transaction.WithSenderAndRecipient
import org.scalatest.concurrent.Eventually
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ContractValidatorResultsV1Spec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with Eventually with WithSenderAndRecipient
    with ContractValidatorResultsGen {

  "ContractValidatorResultsV1Spec" - {
    import com.wavesenterprise.network.message.MessageSpec.ContractValidatorResultsV1Spec._

    "deserializeData(serializedData(data)) == data" in forAll(contractValidatorResultsV1Gen) { message =>
      val restoredMessage = deserializeData(serializeData(message)).get
      restoredMessage shouldBe message
      restoredMessage.signatureValid() shouldBe true
    }
  }
}
