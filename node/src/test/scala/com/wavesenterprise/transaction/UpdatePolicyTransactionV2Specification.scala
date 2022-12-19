package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.{OpType, PermissionsGen}
import com.wavesenterprise.api.http.SignedUpdatePolicyRequestV2
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{TestFees, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertDiffAndState, assertDiffEither, assertLeft}
import com.wavesenterprise.state.{ByteStr, PolicyDiffValue}
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

// TODO 12.03.2020 izhavoronkov: unignore tests
class UpdatePolicyTransactionV2Specification extends AnyFunSpecLike with ScalaCheckPropertyChecks with Matchers with TransactionGen with Inside {

  private val nodeName                 = Some("NodeName")
  private val defaultPolicyId: ByteStr = ByteStr(Base58.decode("thfgsdfewe").get)
  private val registerNodeFee          = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)
  private val updatePolicyFee          = TestFees.defaultFees.forTxType(CreatePolicyTransactionV2.typeId)

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

  def badProof(): Proofs = {
    val bidSign = ByteStr((1 to SignatureLength).map(_ => Random.nextPrintableChar().toByte).toArray)
    Proofs(Seq(bidSign))
  }

  ignore("wrong proofs") {
    forAll(createPolicyTransactionV2GenWithRecipients(), severalPrivKeysGenerator()) {
      case (createPolicyTxWithRecipientPrivKeys, inUpdatePolicyRecipients) =>
        val createPolicyTx           = createPolicyTxWithRecipientPrivKeys.txWrap.tx
        val policyOwner              = createPolicyTxWithRecipientPrivKeys.txWrap.policyOwner
        val inCreatePolicyRecipients = createPolicyTxWithRecipientPrivKeys.recipientsPriKey

        val regNodeSigner = accountGen.sample.get
        val regRecipientsAsParticipants = (inCreatePolicyRecipients ++ inUpdatePolicyRecipients).map { recipient =>
          RegisterNodeTransactionV1
            .selfSigned(regNodeSigner, recipient, nodeName, OpType.Add, System.currentTimeMillis(), registerNodeFee)
            .explicitGet()
        }
        val genesisTx1              = GenesisTransaction.create(createPolicyTx.sender.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
        val genesisTx2              = GenesisTransaction.create(regNodeSigner.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
        val genesisBlock            = TestBlock.create(Seq(genesisTx1, genesisTx2))
        val regRecipientsBlock      = TestBlock.create(regRecipientsAsParticipants)
        val blockWithCreatePolicyTx = TestBlock.create(Seq(createPolicyTx))
        val preconditions           = Seq(genesisBlock, regRecipientsBlock, blockWithCreatePolicyTx)

        val updatePolicyTx =
          updatePolicyTransactionV2GenWithRecipients(policyOwner,
                                                     Gen.const(createPolicyTx.id.value.arr),
                                                     inUpdatePolicyRecipients,
                                                     createPolicyTx.owners.tail).sample.get

        val blockWithUpdatePolicy             = TestBlock.create(Seq(updatePolicyTx))
        val blockWithUpdatePolicy_wrongProofs = TestBlock.create(Seq(updatePolicyTx.copy(proofs = badProof())))

        assertDiffEither(preconditions, blockWithUpdatePolicy, TestFunctionalitySettings.Enabled) { totalDiffEi =>
          inside(totalDiffEi) {
            case Right(diff) =>
              val policy = diff.policies.get(updatePolicyTx.policyId)
              policy should be(defined)
              val policyRegValue = policy.get.asInstanceOf[PolicyDiffValue]
              updatePolicyTx.opType match {
                case OpType.Add =>
                  policyRegValue.ownersToAdd should contain theSameElementsAs updatePolicyTx.owners
                  policyRegValue.ownersToRemove shouldBe empty
                  policyRegValue.recipientsToAdd should contain theSameElementsAs updatePolicyTx.recipients
                  policyRegValue.recipientsToRemove shouldBe empty
                case OpType.Remove =>
                  policyRegValue.ownersToAdd shouldBe empty
                  policyRegValue.ownersToRemove should contain theSameElementsAs updatePolicyTx.owners
                  policyRegValue.recipientsToRemove should contain theSameElementsAs updatePolicyTx.recipients
                  policyRegValue.recipientsToAdd shouldBe empty
              }

          }
        }

        assertLeft(preconditions, blockWithUpdatePolicy_wrongProofs, TestFunctionalitySettings.Enabled)(
          "Script doesn't exist and proof doesn't validate as signature")
    }
  }

  ignore("create policy and update policy transactions in same block") {
    forAll(createPolicyTransactionV2GenWithRecipients(), severalPrivKeysGenerator()) {
      case (createPolicyTxWithRecipientPrivKeys, inUpdatePolicyRecipients) =>
        val createPolicyTx           = createPolicyTxWithRecipientPrivKeys.txWrap.tx
        val policyOwner              = createPolicyTxWithRecipientPrivKeys.txWrap.policyOwner
        val inCreatePolicyRecipients = createPolicyTxWithRecipientPrivKeys.recipientsPriKey

        val regNodeSigner = accountGen.sample.get
        val regRecipientsAsParticipants = (inCreatePolicyRecipients ++ inUpdatePolicyRecipients).map { recipient =>
          RegisterNodeTransactionV1
            .selfSigned(regNodeSigner, recipient, nodeName, OpType.Add, System.currentTimeMillis(), registerNodeFee)
            .explicitGet()
        }
        val genesisTx1         = GenesisTransaction.create(createPolicyTx.sender.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
        val genesisTx2         = GenesisTransaction.create(regNodeSigner.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
        val genesisBlock       = TestBlock.create(Seq(genesisTx1, genesisTx2))
        val regRecipientsBlock = TestBlock.create(regRecipientsAsParticipants)
        val preconditions      = Seq(genesisBlock, regRecipientsBlock)

        val updatePolicyTx =
          updatePolicyTransactionV2GenWithRecipients(policyOwner,
                                                     Gen.const(createPolicyTx.id.value.arr),
                                                     inUpdatePolicyRecipients,
                                                     createPolicyTx.owners.tail).sample.get
        val blockWithCreateAndUpdatePolicy = TestBlock.create(Seq(createPolicyTx, updatePolicyTx))

        assertDiffAndState(preconditions, blockWithCreateAndUpdatePolicy, TestFunctionalitySettings.Enabled) {
          case (diff, _) =>
            val policy = diff.policies.get(createPolicyTx.id.value)
            policy should be(defined)
            val policyRegValue = policy.get.asInstanceOf[PolicyDiffValue]
            updatePolicyTx.opType match {
              case OpType.Add =>
                policyRegValue.ownersToAdd should contain theSameElementsAs updatePolicyTx.owners ++ createPolicyTx.owners
                policyRegValue.ownersToRemove shouldBe empty
                policyRegValue.recipientsToAdd should contain theSameElementsAs updatePolicyTx.recipients ++ createPolicyTx.recipients
                policyRegValue.recipientsToRemove shouldBe empty
              case OpType.Remove =>
                val ownersIntersect     = createPolicyTx.owners.intersect(updatePolicyTx.owners)
                val recipientsIntersect = createPolicyTx.recipients.intersect(updatePolicyTx.recipients)

                policyRegValue.ownersToAdd should contain theSameElementsAs createPolicyTx.owners.toSet -- ownersIntersect
                policyRegValue.ownersToRemove should contain theSameElementsAs updatePolicyTx.owners.toSet -- ownersIntersect
                policyRegValue.recipientsToAdd should contain theSameElementsAs createPolicyTx.recipients.toSet -- recipientsIntersect
                policyRegValue.recipientsToRemove should contain theSameElementsAs updatePolicyTx.recipients.toSet -- recipientsIntersect
            }
        }
    }
  }

  ignore("create policy then remove all owners except one in first update, in second update try to remove last owner") {
    forAll(createPolicyTransactionV2GenWithRecipients().filter(_.txWrap.tx.owners.length > 1)) { createPolicyTxWithRecipientPrivKeys =>
      val createPolicyTx           = createPolicyTxWithRecipientPrivKeys.txWrap.tx
      val policyOwner              = createPolicyTxWithRecipientPrivKeys.txWrap.policyOwner
      val inCreatePolicyRecipients = createPolicyTxWithRecipientPrivKeys.recipientsPriKey

      val regNodeSigner = accountGen.sample.get
      val regRecipientsAsParticipants = inCreatePolicyRecipients.map { recipient =>
        RegisterNodeTransactionV1
          .selfSigned(regNodeSigner, recipient, nodeName, OpType.Add, System.currentTimeMillis(), registerNodeFee)
          .explicitGet()
      }
      val genesisTx1         = GenesisTransaction.create(createPolicyTx.sender.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
      val genesisTx2         = GenesisTransaction.create(regNodeSigner.toAddress, ENOUGH_AMT, System.currentTimeMillis()).explicitGet()
      val genesisBlock       = TestBlock.create(Seq(genesisTx1, genesisTx2))
      val regRecipientsBlock = TestBlock.create(regRecipientsAsParticipants :+ createPolicyTx)
      val preconditions      = Seq(genesisBlock, regRecipientsBlock)

      val firstOwnersToRemove  = createPolicyTx.owners.tail
      val secondOwnersToRemove = createPolicyTx.owners.head

      val firstUpdatePolicyTx =
        updatePolicyTransactionV2GenWithOwners(policyOwner,
                                               Gen.const(createPolicyTx.id.value.arr),
                                               Gen.const(OpType.Remove),
                                               firstOwnersToRemove).sample.get
      val secondUpdatePolicyTx =
        updatePolicyTransactionV2GenWithOwners(policyOwner,
                                               Gen.const(createPolicyTx.id.value.arr),
                                               Gen.const(OpType.Remove),
                                               List(secondOwnersToRemove)).sample.get

      assertDiffAndState(preconditions, TestBlock.create(Seq(firstUpdatePolicyTx)), TestFunctionalitySettings.Enabled) {
        case (_, blockchain) =>
          blockchain.policyExists(createPolicyTx.id.value) shouldBe true
          val policyOwners     = blockchain.policyOwners(createPolicyTx.id.value)
          val policyRecipients = blockchain.policyRecipients(createPolicyTx.id.value)
          policyOwners should contain theSameElementsAs List(secondOwnersToRemove)
          firstUpdatePolicyTx.opType match {
            case OpType.Add    => policyRecipients should contain theSameElementsAs createPolicyTx.recipients ++ firstUpdatePolicyTx.recipients
            case OpType.Remove => policyRecipients should contain theSameElementsAs createPolicyTx.recipients.toSet -- firstUpdatePolicyTx.recipients
          }
      }

      assertLeft(preconditions, TestBlock.create(Seq(firstUpdatePolicyTx, secondUpdatePolicyTx)), TestFunctionalitySettings.Enabled)(
        "Forbidden to remove all policy owners")
    }
  }

}
