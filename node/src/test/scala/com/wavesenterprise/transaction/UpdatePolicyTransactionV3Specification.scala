package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.PermissionsGen
import com.wavesenterprise.api.http.SignedUpdatePolicyRequestV2
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.Base58
import org.scalatest.Inside
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class UpdatePolicyTransactionV3Specification extends AnyFunSpecLike with ScalaCheckPropertyChecks with Matchers with TransactionGen with Inside {

  private val defaultPolicyId: ByteStr = ByteStr(Base58.decode("thfgsdfewe").get)
  private val updatePolicyFee          = TestFees.defaultFees.forTxType(CreatePolicyTransaction.typeId)

  it("deserialize from json works") {
    val sender            = accountGen.sample.get
    val timestamp         = System.currentTimeMillis()
    val proofStr          = "5NxNhjMrrH5EWjSFnVnPbanpThic6fnNL48APVAkwq19y2FpQp4tNSqoAZgboC2ykUfqQs9suwBQj6wERmsWWNqa"
    val recipientsMaxSize = 20
    val ownersMaxSize     = recipientsMaxSize
    val recipients        = severalAddressGenerator(0, recipientsMaxSize).sample.get
    val owners            = severalAddressGenerator(0, ownersMaxSize).sample.get
    val opType            = PermissionsGen.permissionOpTypeGen.sample.get
    val tx = UpdatePolicyTransactionV2(sender,
                                       defaultPolicyId,
                                       recipients,
                                       owners,
                                       opType,
                                       timestamp,
                                       updatePolicyFee,
                                       None,
                                       Proofs(Seq(ByteStr.decodeBase58(proofStr).get)))

    val signedUpdatePolicyRequest = SignedUpdatePolicyRequestV2(
      senderPublicKey = tx.sender.publicKeyBase58,
      policyId = Base58.encode(tx.policyId.arr),
      recipients = tx.recipients.map(_.stringRepr),
      owners = tx.owners.map(_.stringRepr),
      opType = tx.opType.str,
      timestamp = tx.timestamp,
      fee = updatePolicyFee,
      None,
      proofs = tx.proofs.proofs.map(_.toString)
    )

    val deserializedFromJson = tx.json().as[SignedUpdatePolicyRequestV2]
    deserializedFromJson.proofs should contain theSameElementsAs signedUpdatePolicyRequest.proofs
    deserializedFromJson shouldEqual signedUpdatePolicyRequest
  }
}
