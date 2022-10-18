package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{TestFees, TestFunctionalitySettings}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertDiffEither, assertLeft}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class PolicyDataHashV1Specification extends AnyFunSpecLike with ScalaCheckPropertyChecks with Matchers with TransactionGen with Inside {

  private val registerNodeFee = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)

  it("wrong proofs") {
    forAll(createPolicyTransactionV1GenWithRecipients()) { createPolicyTxWithRecipientPrivKeys =>
      val createPolicyTx           = createPolicyTxWithRecipientPrivKeys.txWrap.tx
      val inCreatePolicyRecipients = createPolicyTxWithRecipientPrivKeys.recipientsPriKey

      val regNodeSigner = accountGen.sample.get
      val regRecipientsAsParticipants = inCreatePolicyRecipients.map { recipient =>
        RegisterNodeTransactionV1
          .selfSigned(regNodeSigner, recipient, Some("NodeName"), OpType.Add, System.currentTimeMillis(), registerNodeFee)
          .explicitGet()
      }
      val PolicyDataWithTxV1(_, policyDataHashTx) = policyDataHashTransactionV1Gen(
        policyIdGen = Gen.const(createPolicyTx.id.value.arr),
        senderGen = Gen.const(inCreatePolicyRecipients.head)
      ).sample.get

      val genesisTx1   = GenesisTransaction.create(createPolicyTx.sender.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
      val genesisTx2   = GenesisTransaction.create(policyDataHashTx.sender.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
      val genesisTx3   = GenesisTransaction.create(regNodeSigner.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
      val genesisBlock = TestBlock.create(Seq(genesisTx1, genesisTx2, genesisTx3))

      val regRecipientsBlock      = TestBlock.create(regRecipientsAsParticipants)
      val blockWithCreatePolicyTx = TestBlock.create(Seq(createPolicyTx))
      val preconditions           = Seq(genesisBlock, regRecipientsBlock, blockWithCreatePolicyTx)

      val blockWithPolicyDataHash             = TestBlock.create(Seq(policyDataHashTx))
      val blockWithPolicyDataHash_wrongProofs = TestBlock.create(Seq(policyDataHashTx.copy(proofs = badProof())))

      assertDiffEither(preconditions, blockWithPolicyDataHash, TestFunctionalitySettings.Enabled) { totalDiffEi =>
        inside(totalDiffEi) {
          case Right(diff) =>
            val policyDataHashes = diff.policiesDataHashes.get(policyDataHashTx.policyId)
            policyDataHashes should be(defined)
            val dataHashes = policyDataHashes.get
            dataHashes should contain theSameElementsAs Set(policyDataHashTx)
        }
      }

      assertLeft(preconditions, blockWithPolicyDataHash_wrongProofs, TestFunctionalitySettings.Enabled)(
        "Script doesn't exist and proof doesn't validate as signature")
    }
  }

  def badProof(): Proofs = {
    val bidSign = ByteStr((1 to SignatureLength).map(_ => Random.nextPrintableChar().toByte).toArray)
    Proofs(Seq(bidSign))
  }
}
