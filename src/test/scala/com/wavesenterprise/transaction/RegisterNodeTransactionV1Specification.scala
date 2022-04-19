package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.api.http.SignedRegisterNodeRequest
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.ByteStr
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RegisterNodeTransactionV1Specification extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {
  val regNodeTxFee: Long = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)

  property("deserialize from json works") {
    val sender    = accountGen.sample.get
    val target    = accountGen.sample.get
    val nodeName  = Some("node-name")
    val opType    = OpType.Add
    val timestamp = System.currentTimeMillis()
    val proofStr  = "5NxNhjMrrH5EWjSFnVnPbanpThic6fnNL48APVAkwq19y2FpQp4tNSqoAZgboC2ykUfqQs9suwBQj6wERmsWWNqa"
    val tx = RegisterNodeTransactionV1(
      sender = sender,
      target = target,
      nodeName = nodeName,
      opType = opType,
      timestamp = timestamp,
      fee = regNodeTxFee,
      proofs = Proofs(Seq(ByteStr.decodeBase58(proofStr).get))
    )

    val signedRegisterNodeRequest = SignedRegisterNodeRequest(
      senderPublicKey = tx.sender.publicKeyBase58,
      targetPubKey = tx.target.publicKeyBase58,
      opType = tx.opType.str,
      nodeName = nodeName,
      timestamp = tx.timestamp,
      fee = regNodeTxFee,
      proofs = tx.proofs.proofs.map(_.toString)
    )

    val deserializedFromJson = tx.json().as[SignedRegisterNodeRequest]
    deserializedFromJson.proofs should contain theSameElementsAs signedRegisterNodeRequest.proofs
    deserializedFromJson shouldEqual signedRegisterNodeRequest
  }
}
