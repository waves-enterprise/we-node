package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.api.http.SignedRegisterNodeRequestV2
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.ByteStr
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RegisterNodeTransactionV2Specification extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {
  val regNodeTxFee: Long = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)

  property("deserialize from json works") {
    val sender      = accountGen.sample.get
    val target      = accountGen.sample.get
    val nodeName    = Some("node-name")
    val opType      = OpType.Add
    val timestamp   = System.currentTimeMillis()
    val atomicBadge = Some(AtomicBadge(Some(sender.toAddress)))
    val proofStr    = "5NxNhjMrrH5EWjSFnVnPbanpThic6fnNL48APVAkwq19y2FpQp4tNSqoAZgboC2ykUfqQs9suwBQj6wERmsWWNqa"
    val tx = RegisterNodeTransactionV2(
      sender = sender,
      target = target,
      nodeName = nodeName,
      opType = opType,
      timestamp = timestamp,
      fee = regNodeTxFee,
      atomicBadge = atomicBadge,
      proofs = Proofs(Seq(ByteStr.decodeBase58(proofStr).get))
    )

    val signedRegisterNodeRequest = SignedRegisterNodeRequestV2(
      senderPublicKey = tx.sender.publicKeyBase58,
      targetPubKey = tx.target.publicKeyBase58,
      opType = tx.opType.str,
      nodeName = nodeName,
      timestamp = tx.timestamp,
      fee = regNodeTxFee,
      atomicBadge = atomicBadge,
      proofs = tx.proofs.proofs.map(_.toString)
    )

    val deserializedFromJson = tx.json().as[SignedRegisterNodeRequestV2]
    deserializedFromJson.proofs should contain theSameElementsAs signedRegisterNodeRequest.proofs
    deserializedFromJson.atomicBadge shouldEqual atomicBadge
    deserializedFromJson shouldEqual signedRegisterNodeRequest
  }
}
