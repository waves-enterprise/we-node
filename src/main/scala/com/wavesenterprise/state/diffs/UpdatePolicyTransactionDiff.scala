package com.wavesenterprise.state.diffs

import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.state.diffs.UpdatePolicyTransactionDiff._
import com.wavesenterprise.state.{Blockchain, Diff, PolicyDiffValue}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{UpdatePolicyTransaction, ValidationError}

case class UpdatePolicyTransactionDiff(blockchain: Blockchain, height: Int) {

  def apply(tx: UpdatePolicyTransaction): Either[ValidationError, Diff] = {
    for {
      _   <- PolicyDiff.checkPolicyIdEmptiness(tx.policyId)
      _   <- checkRecipientsAndOwnersEmptiness(tx)
      _   <- PolicyDiff.checkPolicyExistence(tx.policyId, blockchain)
      res <- diffExistingPolicy(tx, blockchain, height)
    } yield res
  }

}

object UpdatePolicyTransactionDiff {
  private def checkRecipientsAndOwnersEmptiness(tx: UpdatePolicyTransaction): Either[ValidationError, Unit] = {
    if (tx.owners.isEmpty && tx.recipients.isEmpty) {
      Left(GenericError(s"Invalid UpdatePolicyTransaction: empty owners and recipients"))
    } else {
      Right(())
    }
  }

  private def diffExistingPolicy(tx: UpdatePolicyTransaction, blockchain: Blockchain, height: Int): Either[ValidationError, Diff] = {
    for {
      _ <- checkNewParticipants(tx, blockchain)
      _ <- checkOwners(tx, blockchain)
      _ <- checkCollisions(tx, blockchain)
    } yield
      Diff(
        height,
        tx,
        portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress, blockchain),
        policies = Map(tx.policyId -> PolicyDiffValue.fromTx(tx))
      )
  }

  private def checkNewParticipants(tx: UpdatePolicyTransaction, blockchain: Blockchain): Either[ValidationError, Unit] = tx.opType match {
    case OpType.Add =>
      val networkParticipants                = blockchain.networkParticipants()
      val allRecipientsIsNetworkParticipants = tx.recipients.forall(networkParticipants.contains)
      if (!allRecipientsIsNetworkParticipants) {
        val unacceptedRecipients = tx.recipients.filterNot(networkParticipants.contains)
        Left(GenericError(s"Unaccepted recipients in policy update: [${unacceptedRecipients.mkString(", ")}]"))
      } else {
        Right(())
      }
    case OpType.Remove => Right(())
  }

  private def checkOwners(tx: UpdatePolicyTransaction, blockchain: Blockchain): Either[ValidationError, Unit] = {
    val policyOwners = blockchain.policyOwners(tx.policyId)
    for {
      _ <- Either.cond(
        policyOwners.contains(tx.sender.toAddress),
        (),
        GenericError(s"Sender '${tx.sender.address}' is not owner for policy with id '${tx.policyId.toString}'")
      )
      newPolicyOwners = combineOwners(policyOwners, tx.owners, tx.opType)
      res <- Either.cond(newPolicyOwners.nonEmpty, (), GenericError("Forbidden to remove all policy owners"))
    } yield res
  }

  private def combineOwners(inPolicy: Set[Address], inTransaction: List[Address], opType: OpType): Set[Address] =
    opType match {
      case OpType.Add    => inPolicy ++ inTransaction
      case OpType.Remove => inPolicy -- inTransaction
    }

  private def checkCollisions(tx: UpdatePolicyTransaction, blockchain: Blockchain): Either[ValidationError, Unit] = {
    val currentPolicyOwners     = blockchain.policyOwners(tx.policyId)
    val currentPolicyRecipients = blockchain.policyRecipients(tx.policyId)

    tx.opType match {
      case OpType.Remove =>
        Either.cond(
          tx.owners.forall(currentPolicyOwners.contains) && tx.recipients.forall(currentPolicyRecipients.contains),
          (),
          GenericError("Cannot remove owners or recipients that are absent from the policy")
        )
      case OpType.Add =>
        Either.cond(
          !tx.owners.exists(currentPolicyOwners.contains) && !tx.recipients.exists(currentPolicyRecipients.contains),
          (),
          GenericError("Cannot add owners or recipients that are already in the policy")
        )

    }
  }
}
