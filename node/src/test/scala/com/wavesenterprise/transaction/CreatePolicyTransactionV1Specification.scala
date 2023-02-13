package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.api.http.SignedCreatePolicyRequestV1
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{TestFees, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertDiffEither, assertLeft}
import com.wavesenterprise.state.{ByteStr, PolicyDiffValue}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatest.Inside
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class CreatePolicyTransactionV1Specification extends AnyFunSpecLike with ScalaCheckPropertyChecks with Matchers with TransactionGen with Inside {

  private val registerNodeFee = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)
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
    val tx = CreatePolicyTransactionV1(sender,
                                       policyName,
                                       description,
                                       recipients,
                                       owners,
                                       timestamp,
                                       createPolicyFee,
                                       Proofs(Seq(ByteStr.decodeBase58(proofStr).get)))

    val signedCreatePolicyRequest = SignedCreatePolicyRequestV1(
      tx.sender.publicKeyBase58,
      tx.policyName,
      tx.description,
      tx.recipients.map(_.stringRepr),
      tx.owners.map(_.stringRepr),
      tx.timestamp,
      createPolicyFee,
      tx.proofs.proofs.map(_.toString)
    )

    val deserializedFromJson = tx.json().as[SignedCreatePolicyRequestV1]
    deserializedFromJson.proofs should contain theSameElementsAs signedCreatePolicyRequest.proofs
    deserializedFromJson shouldEqual signedCreatePolicyRequest
  }

  def badProof(): Proofs = {
    val bidSign = ByteStr((1 to SignatureLength).map(_ => Random.nextPrintableChar().toByte).toArray)
    Proofs(Seq(bidSign))
  }

  it("wrong proofs") {
    forAll(createPolicyTransactionV1GenWithRecipients()) { createPolicyTxWithRecipients =>
      val createPolicyTx = createPolicyTxWithRecipients.txWrap.tx
      val allRecipients  = createPolicyTxWithRecipients.recipientsPriKey
      val regNodeSender  = accountGen.sample.get

      val regRecipientsAsParticipants = allRecipients.map { recipient =>
        RegisterNodeTransactionV1
          .selfSigned(regNodeSender, recipient, Some("NodeName"), OpType.Add, System.currentTimeMillis(), registerNodeFee)
          .explicitGet()
      }

      val genesisTxs =
        Seq(
          GenesisTransaction.create(regNodeSender.toAddress, ENOUGH_AMT * 2, System.currentTimeMillis()).explicitGet(),
          GenesisTransaction.create(createPolicyTx.sender.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
        )

      val genesisBlock                      = TestBlock.create(genesisTxs)
      val regRecipientsBlock                = TestBlock.create(regRecipientsAsParticipants)
      val blockWithCreatePolicy             = TestBlock.create(Seq(createPolicyTx))
      val blockWithCreatePolicy_wrongProofs = TestBlock.create(Seq(createPolicyTx.copy(proofs = badProof())))

      assertDiffEither(Seq(genesisBlock, regRecipientsBlock), blockWithCreatePolicy, TestFunctionalitySettings.Enabled) { totalDiffEi =>
        inside(totalDiffEi) {
          case Right(diff) =>
            val policy = diff.policies.get(createPolicyTx.id.value)
            policy should be(defined)
            val policyRegValue = policy.get.asInstanceOf[PolicyDiffValue]
            policyRegValue.ownersToAdd should contain theSameElementsAs createPolicyTx.owners
            policyRegValue.ownersToRemove shouldBe empty
            policyRegValue.recipientsToAdd should contain theSameElementsAs createPolicyTx.recipients
            policyRegValue.recipientsToRemove shouldBe empty
        }
      }

      assertLeft(Seq(genesisBlock, regRecipientsBlock), blockWithCreatePolicy_wrongProofs, TestFunctionalitySettings.Enabled)(
        "Script doesn't exist and proof doesn't validate as signature")
    }
  }
}
