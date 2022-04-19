package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.api.http.SignedCreatePolicyRequestV3
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.ByteStr
import org.scalatest.{FunSpecLike, Inside, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CreatePolicyTransactionV3Specification extends FunSpecLike with ScalaCheckPropertyChecks with Matchers with TransactionGen with Inside {

  private val createPolicyFee = TestFees.defaultFees.forTxType(CreatePolicyTransaction.typeId)

  it("deserialize from json works") {
    val sender            = accountGen.sample.get
    val policyName        = "some policy name"
    val description       = "some policy description"
    val timestamp         = System.currentTimeMillis()
    val proofStr          = "5NxNhjMrrH5EWjSFnVnPbanpThic6fnNL48APVAkwq19y2FpQp4tNSqoAZgboC2ykUfqQs9suwBQj6wERmsWWNqa"
    val recipientsMaxSize = 20
    val ownersMaxSize     = recipientsMaxSize
    val recipients        = severalAddressGenerator(0, recipientsMaxSize).sample.get
    val owners            = severalAddressGenerator(0, ownersMaxSize).sample.get
    val tx = CreatePolicyTransactionV3(
      sender,
      policyName,
      description,
      recipients,
      owners,
      timestamp,
      createPolicyFee,
      None,
      Some(AtomicBadge(Some(sender.toAddress))),
      Proofs(Seq(ByteStr.decodeBase58(proofStr).get))
    )

    val signedCreatePolicyRequest = SignedCreatePolicyRequestV3(
      tx.sender.publicKeyBase58,
      tx.policyName,
      tx.description,
      tx.recipients.map(_.stringRepr),
      tx.owners.map(_.stringRepr),
      tx.timestamp,
      createPolicyFee,
      None,
      Some(AtomicBadge(Some(sender.toAddress))),
      tx.proofs.proofs.map(_.toString)
    )

    val deserializedFromJson = tx.json().as[SignedCreatePolicyRequestV3]
    deserializedFromJson.proofs should contain theSameElementsAs signedCreatePolicyRequest.proofs
    deserializedFromJson shouldEqual signedCreatePolicyRequest
  }
}
