package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.settings.TestFees
import com.wavesenterprise.state.diffs.UpdatePolicyTransactionDiff
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff, LeaseBalance, PolicyDiffValue, Portfolio}
import com.wavesenterprise.transaction.ValidationError.{GenericError, PolicyDoesNotExist}
import com.wavesenterprise.transaction.{Proofs, UpdatePolicyTransactionV1}
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FunSpecLike, Matchers}
import tools.GenHelper._

class UpdatePolicyTransactionDiffTest
    extends FunSpecLike
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen
    with MockFactory {

  private val blockChainHeight = 10
  private val fee              = TestFees.defaultFees.forTxType(UpdatePolicyTransactionV1.typeId)

  it("policy update with empty policyId") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction(List.empty, List.empty).copy(policyId = ByteStr(Array.empty))

    updatePolicyDiff(transaction) shouldBe Left(GenericError("PolicyId could not be empty"))
  }

  it("policy update with empty owners & recipients") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction(List.empty, List.empty)

    updatePolicyDiff(transaction) shouldBe Left(GenericError("Invalid UpdatePolicyTransaction: empty owners and recipients"))
  }

  it("update policy that is not exists") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction()
    (blockchain.policyExists _).expects(transaction.policyId).returns(false).once()

    updatePolicyDiff(transaction) shouldBe Left(PolicyDoesNotExist(transaction.policyId))
  }

  it("new recipients is not networkParticipants") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction(opType = OpType.Add)

    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    val someRandomAddresses = severalAddressGenerator().generateSample()
    (blockchain.networkParticipants _).expects().returning(someRandomAddresses).once()

    val diffResult = updatePolicyDiff(transaction)
    diffResult.isLeft shouldBe true
    diffResult.left.get.asInstanceOf[GenericError].err should include("Unaccepted recipients in policy update")
  }

  it("fail to remove all policy owners") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction(opType = OpType.Remove, senderInOwners = true)
    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.policyOwners _).expects(transaction.policyId).returning(transaction.owners.toSet).once()

    updatePolicyDiff(transaction) shouldBe Left(GenericError("Forbidden to remove all policy owners"))
  }

  it("successfully create and update policy transaction (OpType.Remove)") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction(opType = OpType.Remove, owners = List.empty)

    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.policyOwners _).expects(transaction.policyId).returning(Set(transaction.sender.toAddress)).twice()
    (blockchain.policyRecipients _).expects(transaction.policyId).returning(transaction.recipients.toSet).once()

    updatePolicyDiff(transaction) shouldBe Right(
      Diff(
        blockChainHeight,
        transaction,
        portfolios = Map(transaction.sender.toAddress -> Portfolio(-transaction.fee, LeaseBalance.empty, Map.empty)),
        policies = Map(transaction.policyId           -> PolicyDiffValue(Set.empty, Set.empty, transaction.owners.toSet, transaction.recipients.toSet))
      ))
  }

  it("successfully create add update policy transaction (OpType.Add)") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction(opType = OpType.Add)

    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.networkParticipants _).expects().returning(transaction.recipients).once()
    val someRandomAddresses = severalAddressGenerator().generateSample()
    (blockchain.policyOwners _).expects(transaction.policyId).returning((someRandomAddresses :+ transaction.sender.toAddress).toSet).twice()
    (blockchain.policyRecipients _).expects(transaction.policyId).returning(someRandomAddresses.toSet).once()

    updatePolicyDiff(transaction) shouldBe Right(
      Diff(
        blockChainHeight,
        transaction,
        portfolios = Map(transaction.sender.toAddress -> Portfolio(-transaction.fee, LeaseBalance.empty, Map.empty)),
        policies = Map(transaction.policyId           -> PolicyDiffValue(transaction.owners.toSet, transaction.recipients.toSet, Set.empty, Set.empty))
      ))
  }

  it("fail to add already presented owners or recipients") {
    val blockchain          = mock[Blockchain]
    val updatePolicyDiff    = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction         = updatePolicyTransaction(opType = OpType.Add)
    val someRandomAddresses = severalAddressGenerator().generateSample()

    // for recipients
    {
      (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
      (blockchain.networkParticipants _).expects().returning(transaction.recipients).once()
      val policiesWithCollision = transaction.recipients.head +: someRandomAddresses
      (blockchain.policyOwners _).expects(transaction.policyId).returning((someRandomAddresses :+ transaction.sender.toAddress).toSet).twice()
      (blockchain.policyRecipients _).expects(transaction.policyId).returning(policiesWithCollision.toSet).once()

      val diffResult = updatePolicyDiff(transaction)
      diffResult.isLeft shouldBe true
      diffResult.left.get.asInstanceOf[GenericError].err shouldBe "Cannot add owners or recipients that are already in the policy"
    }

    // for owners
    {
      (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
      (blockchain.networkParticipants _).expects().returning(transaction.recipients).once()
      val ownersWithCollision = transaction.owners.head +: someRandomAddresses
      (blockchain.policyOwners _).expects(transaction.policyId).returning((ownersWithCollision :+ transaction.sender.toAddress).toSet).twice()
      (blockchain.policyRecipients _).expects(transaction.policyId).returning(someRandomAddresses.toSet).once()

      val diffResult = updatePolicyDiff(transaction)
      diffResult.isLeft shouldBe true
      diffResult.left.get.asInstanceOf[GenericError].err shouldBe "Cannot add owners or recipients that are already in the policy"
    }
  }

  it("fail to remove not presented owners or recipients") {
    val blockchain          = mock[Blockchain]
    val updatePolicyDiff    = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction         = updatePolicyTransaction(opType = OpType.Remove)
    val someRandomAddresses = severalAddressGenerator().sample.get

    // for recipients
    {
      (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
      (blockchain.policyOwners _).expects(transaction.policyId).returning((transaction.sender.toAddress :: transaction.owners).toSet).twice()
      (blockchain.policyRecipients _).expects(transaction.policyId).returning(someRandomAddresses.toSet).once()

      val diffResult = updatePolicyDiff(transaction)
      diffResult.isLeft shouldBe true
      diffResult.left.get.asInstanceOf[GenericError].err shouldBe "Cannot remove owners or recipients that are absent from the policy"
    }

    // for owners
    {
      (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
      (blockchain.policyOwners _).expects(transaction.policyId).returning(Set(transaction.sender.toAddress)).twice()
      (blockchain.policyRecipients _).expects(transaction.policyId).returning(transaction.recipients.toSet).once()

      val diffResult = updatePolicyDiff(transaction)
      diffResult.isLeft shouldBe true
      diffResult.left.get.asInstanceOf[GenericError].err shouldBe "Cannot remove owners or recipients that are absent from the policy"
    }
  }

  it("fail to update policy if transaction sender is not policy owner") {
    val blockchain       = mock[Blockchain]
    val updatePolicyDiff = UpdatePolicyTransactionDiff(blockchain, blockChainHeight)
    val transaction      = updatePolicyTransaction(opType = OpType.Add)

    (blockchain.policyExists _).expects(transaction.policyId).returns(true).once()
    (blockchain.networkParticipants _).expects().returning(transaction.recipients).once()
    val someRandomAddresses = severalAddressGenerator().sample.get
    (blockchain.policyOwners _).expects(transaction.policyId).returning(someRandomAddresses.toSet).once()

    val diffResult = updatePolicyDiff(transaction)
    diffResult.isLeft shouldBe true
    diffResult.left.get
      .asInstanceOf[GenericError]
      .err shouldBe s"Sender '${transaction.sender.address}' is not owner for policy with id '${transaction.policyId.toString}'"
  }

  private def updatePolicyTransaction(recipients: List[Address] = severalAddressGenerator().generateSample(),
                                      owners: List[Address] = severalAddressGenerator().generateSample(),
                                      opType: OpType = OpType.Remove,
                                      senderInOwners: Boolean = false): UpdatePolicyTransactionV1 = {
    val sender       = accountGen.generateSample()
    val policyOwners = if (senderInOwners) owners :+ sender.toAddress else owners
    UpdatePolicyTransactionV1(
      sender,
      ByteStr(genBoundedString(10, 100).generateSample()),
      recipients,
      policyOwners,
      opType,
      System.currentTimeMillis(),
      fee,
      Proofs.empty
    )
  }
}
