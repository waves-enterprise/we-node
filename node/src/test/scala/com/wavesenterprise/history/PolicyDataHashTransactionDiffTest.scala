package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.PolicyDataHashTransactionDiff
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError.{GenericError, PolicyDataHashAlreadyExists, PolicyDoesNotExist, SenderIsNotInPolicyRecipients}
import com.wavesenterprise.transaction.{PolicyDataHashTransactionV1, Proofs}
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class PolicyDataHashTransactionDiffTest
    extends AnyFunSpecLike
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen
    with MockFactory {

  private val blockChainHeight               = 10
  private val policyDataHash: PolicyDataHash = PolicyDataHash.fromDataBytes(byteArrayGen(66600).sample.get)
  private val fee                            = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)

  it("policy update with empty policyId") {
    val blockchain         = mock[Blockchain]
    val policyDataHashDiff = PolicyDataHashTransactionDiff(blockchain, blockChainHeight)
    val transaction        = createDataHashTransaction().copy(policyId = ByteStr(Array.empty))
    policyDataHashDiff(transaction) shouldBe Left(GenericError("PolicyId could not be empty"))
  }

  it("add data hash to policy that is not exists") {
    val blockchain         = mock[Blockchain]
    val policyDataHashDiff = PolicyDataHashTransactionDiff(blockchain, blockChainHeight)
    val transaction        = createDataHashTransaction()
    (blockchain.policyExists _).expects(transaction.policyId).returns(false).once()

    policyDataHashDiff(transaction) shouldBe Left(PolicyDoesNotExist(transaction.policyId))
  }

  it("policy data hash already exists") {
    val blockchain         = mock[Blockchain]
    val policyDataHashDiff = PolicyDataHashTransactionDiff(blockchain, blockChainHeight)
    val transaction        = createDataHashTransaction()
    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.policyDataHashExists _).expects(transaction.policyId, transaction.dataHash).returns(true).once()

    policyDataHashDiff(transaction) shouldBe Left(PolicyDataHashAlreadyExists(transaction.dataHash))
  }

  it("sender is not in policy recipients before atomic feature") {
    val blockchain         = mock[Blockchain]
    val policyDataHashDiff = PolicyDataHashTransactionDiff(blockchain, blockChainHeight)
    val transaction        = createDataHashTransaction()
    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.policyRecipients _).expects(transaction.policyId).returns(Set.empty).once()
    (blockchain.policyDataHashExists _).expects(transaction.policyId, transaction.dataHash).returns(false).once()
    (blockchain.activatedFeatures _).expects().returns(Map.empty).once()
    (blockchain.height _).expects().returns(1).once()

    policyDataHashDiff(transaction) shouldBe 'right
  }

  it("sender is not in policy recipients after atomic feature") {
    val blockchain         = mock[Blockchain]
    val policyDataHashDiff = PolicyDataHashTransactionDiff(blockchain, blockChainHeight)
    val transaction        = createDataHashTransaction()
    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.policyRecipients _).expects(transaction.policyId).returns(Set.empty).once()
    (blockchain.policyDataHashExists _).expects(transaction.policyId, transaction.dataHash).returns(false).once()

    val AtomicTransactionSupportHeight = 4
    (blockchain.activatedFeatures _)
      .expects()
      .returns(Map(BlockchainFeature.AtomicTransactionSupport.id -> AtomicTransactionSupportHeight))
      .once()
    (blockchain.height _).expects().returns(AtomicTransactionSupportHeight).once()

    policyDataHashDiff(transaction) shouldBe Left(SenderIsNotInPolicyRecipients(transaction.sender.toAddress.toString))
  }

  it("successfully create") {
    val blockchain         = mock[Blockchain]
    val policyDataHashDiff = PolicyDataHashTransactionDiff(blockchain, blockChainHeight)
    val transaction        = createDataHashTransaction()

    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.policyRecipients _).expects(transaction.policyId).returns(Set(transaction.sender.toAddress)).once()
    (blockchain.policyDataHashExists _).expects(transaction.policyId, transaction.dataHash).returns(false).once()
    policyDataHashDiff(transaction) shouldBe Right(
      Diff(
        blockChainHeight,
        transaction,
        portfolios = Map(transaction.sender.toAddress.toAssetHolder -> Portfolio(-transaction.fee, LeaseBalance.empty, Map.empty)),
        policiesDataHashes = Map(transaction.policyId -> Set(transaction)),
        dataHashToSender = Map(transaction.dataHash -> transaction.sender.toAddress)
      ))
  }

  private def createDataHashTransaction(): PolicyDataHashTransactionV1 = {
    PolicyDataHashTransactionV1(
      accountGen.sample.get,
      policyDataHash,
      ByteStr(genBoundedString(10, 100).sample.get),
      System.currentTimeMillis(),
      fee,
      Proofs.empty
    )
  }
}
