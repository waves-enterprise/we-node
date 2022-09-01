package com.wavesenterprise.state.diffs

import com.wavesenterprise.state.diffs.CreatePolicyTransactionDiff._
import com.wavesenterprise.state.{Blockchain, Diff, PolicyDiffValue}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.validation.PolicyValidation
import com.wavesenterprise.transaction.{CreatePolicyTransaction, ValidationError}

case class CreatePolicyTransactionDiff(blockchain: Blockchain, height: Int) {

  def apply(tx: CreatePolicyTransaction): Either[ValidationError, Diff] = {
    val policyId = tx.id.value

    for {
      _ <- checkPolicyNameLength(tx)
      _ <- PolicyDiff.checkPolicyIdEmptiness(policyId)
      _ <- checkOwnersEmptiness(tx)
      _ <- checkSenderIsOwner(tx)
      _ <- PolicyDiff.checkPolicyNotExists(policyId, blockchain)
      _ <- CreatePolicyTransactionDiff.checkRecipients(tx, blockchain, height)
    } yield
      Diff(
        height,
        tx,
        portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress, blockchain),
        policies = Map(tx.id.value -> PolicyDiffValue.fromTx(tx))
      )
  }

}

object CreatePolicyTransactionDiff {

  def checkPolicyNameLength(tx: CreatePolicyTransaction): Either[ValidationError, Unit] = {
    Either.cond(tx.policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
  }

  def checkSenderIsOwner(tx: CreatePolicyTransaction): Either[ValidationError, Unit] = {
    Either.cond(tx.owners.contains(tx.sender.toAddress), (), GenericError(s"Sender '${tx.sender.toAddress}' is not owner for policy"))
  }

  def checkOwnersEmptiness(tx: CreatePolicyTransaction): Either[ValidationError, Unit] = {
    Either.cond(tx.owners.nonEmpty, (), GenericError(s"Policy without owners is invalid"))
  }

  def checkRecipients(tx: CreatePolicyTransaction, blockchain: Blockchain, height: Int): Either[ValidationError, Unit] = {
    val networkParticipants                = blockchain.networkParticipants()
    val allRecipientsIsNetworkParticipants = tx.recipients.forall(networkParticipants.contains)
    if (!allRecipientsIsNetworkParticipants) {
      val unacceptedRecipients = tx.recipients.filterNot(networkParticipants.contains)
      Left(GenericError(s"Recipients [${unacceptedRecipients.mkString("; ")}] is not registered in network"))
    } else {
      Right(())
    }
  }

}
