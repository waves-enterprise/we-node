package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.account.Address
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest._
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class CreatePolicyTransactionDiffTest
    extends AnyFunSpecLike
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen
    with MockFactory {

  private val blockChainHeight = 10
  private val fee              = TestFees.defaultFees.forTxType(CreatePolicyTransactionV1.typeId)

  it("fail to create policy without owners") {
    val blockchain       = mock[Blockchain]
    val createPolicyDiff = CreatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = createPolicyTransaction(List.empty, List.empty, senderIsInOwners = false)
    createPolicyDiff(transaction) shouldBe Left(GenericError("Policy without owners is invalid"))
  }

  it("do not create policy if it is already exists") {
    forAll(severalAddressGenerator()) { owners =>
      val blockchain       = mock[Blockchain]
      val createPolicyDiff = CreatePolicyTransactionDiff(blockchain, blockChainHeight)
      val transaction      = createPolicyTransaction(List.empty, owners)
      (blockchain.policyExists _).expects(transaction.id.value).returns(true).once()
      createPolicyDiff(transaction) shouldBe Left(GenericError("Policy already exists"))
    }
  }

  it("some policy recipients is not in network participants") {
    forAll(severalAddressGenerator(), severalAddressGenerator()) { (owners, allRecipients) =>
      val blockchain                                 = mock[Blockchain]
      val createPolicyDiff                           = CreatePolicyTransactionDiff(blockchain, blockChainHeight)
      val transaction                                = createPolicyTransaction(allRecipients, owners)
      val (unacceptedRecipients, acceptedRecipients) = allRecipients.splitAt(allRecipients.length / 2)
      (blockchain.policyExists _).expects(transaction.id.value).returns(false).once()
      (blockchain.networkParticipants _).expects().returns(acceptedRecipients).once()
      createPolicyDiff(transaction) shouldBe Left(GenericError(s"Recipients [${unacceptedRecipients.mkString("; ")}] is not registered in network"))
    }
  }

  it("successfully create") {
    forAll(severalAddressGenerator(), severalAddressGenerator()) { (owners, allRecipients) =>
      val blockchain       = mock[Blockchain]
      val createPolicyDiff = CreatePolicyTransactionDiff(blockchain, blockChainHeight)
      val transaction      = createPolicyTransaction(allRecipients, owners)
      (blockchain.policyExists _).expects(transaction.id.value).returns(false).once()
      (blockchain.networkParticipants _).expects().returns(allRecipients).once()
      createPolicyDiff(transaction) shouldBe Right(
        Diff(
          blockChainHeight,
          transaction,
          portfolios = Map(transaction.sender.toAddress.toAssetHolder -> Portfolio(-transaction.fee, LeaseBalance.empty, Map.empty)),
          policies = Map(transaction.id.value -> PolicyDiffValue(transaction.owners.toSet, transaction.recipients.toSet, Set.empty, Set.empty))
        ))
    }
  }

  it("fail to create policy if sender is not in owners") {
    forAll(severalAddressGenerator(), severalAddressGenerator()) { (owners, allRecipients) =>
      val blockchain       = mock[Blockchain]
      val createPolicyDiff = CreatePolicyTransactionDiff(blockchain, blockChainHeight)
      val transaction      = createPolicyTransaction(allRecipients, owners, senderIsInOwners = false)
      createPolicyDiff(transaction) shouldBe Left(GenericError(s"Sender '${transaction.sender.address}' is not owner for policy"))
    }
  }

  private def createPolicyTransaction(recipients: List[Address],
                                      owners: List[Address],
                                      senderIsInOwners: Boolean = true): CreatePolicyTransactionV1 = {
    val sender       = accountGen.sample.get
    val resultOwners = if (senderIsInOwners) sender.toAddress +: owners else owners
    CreatePolicyTransactionV1(
      sender,
      "some random policy name",
      "some random description",
      recipients,
      resultOwners,
      System.currentTimeMillis(),
      fee,
      Proofs.empty
    )
  }
}
