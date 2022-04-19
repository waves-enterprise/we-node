package com.wavesenterprise.network

import com.wavesenterprise.docker.ContractExecutionMessageGen
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class NetworkContractExecutionMessageSpecification
    extends FreeSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with Eventually
    with ContractExecutionMessageGen {

  private val networkMessageGen: Gen[NetworkContractExecutionMessage] = messageGen().map(NetworkContractExecutionMessage)

  "NetworkContractExecutionMessageSpec" - {
    import com.wavesenterprise.network.message.MessageSpec.NetworkContractExecutionMessageSpec._

    "deserializeData(serializedData(data)) == data" in forAll(networkMessageGen) { message =>
      val restoredMessage = deserializeData(serializeData(message)).get
      restoredMessage shouldBe message
    }
  }
}
