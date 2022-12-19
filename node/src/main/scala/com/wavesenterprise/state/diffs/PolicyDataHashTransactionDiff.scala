package com.wavesenterprise.state.diffs

import com.wavesenterprise.account.Address
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.state.diffs.PolicyDataHashTransactionDiff._
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.transaction.ValidationError.{GenericError, PolicyDataHashAlreadyExists, SenderIsNotInPolicyRecipients}
import com.wavesenterprise.transaction.{PolicyDataHashTransaction, ValidationError}

case class PolicyDataHashTransactionDiff(blockchain: Blockchain, height: Int) {

  def apply(tx: PolicyDataHashTransaction): Either[ValidationError, Diff] = {
    for {
      _ <- PolicyDiff.checkPolicyIdEmptiness(tx.policyId)
      _ <- checkDataHashEmptiness(tx)
      _ <- PolicyDiff.checkPolicyExistence(tx.policyId, blockchain)
      _ <- checkPolicyDataHashExistence(tx.policyId, tx.dataHash, blockchain)
      _ <- checkSenderIsInPolicyRecipients(tx.policyId, tx.sender.toAddress, blockchain)
    } yield Diff(
      height,
      tx,
      portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain),
      policiesDataHashes = Map(tx.policyId -> Set(tx)),
      dataHashToSender = Map(tx.dataHash -> tx.sender.toAddress)
    )
  }

}

object PolicyDataHashTransactionDiff {

  def checkDataHashEmptiness(tx: PolicyDataHashTransaction): Either[ValidationError, Unit] = {
    if (tx.dataHash.bytes.arr.isEmpty) {
      Left(GenericError(s"Hash could not be empty"))
    } else {
      Right(())
    }
  }

  def checkPolicyDataHashExistence(policyId: ByteStr, dataHash: PolicyDataHash, blockchain: Blockchain): Either[ValidationError, Unit] = {
    if (blockchain.policyDataHashExists(policyId, dataHash)) {
      Left(PolicyDataHashAlreadyExists(dataHash))
    } else {
      Right(())
    }
  }

  private def checkSenderIsInPolicyRecipients(policyId: ByteStr, sender: Address, blockchain: Blockchain): Either[ValidationError, Unit] =
    Either.cond(
      blockchain.policyRecipients(policyId).contains(sender) ||
        // The current validation was added after AtomicTransaction implementation
        !blockchain.isFeatureActivated(BlockchainFeature.AtomicTransactionSupport, blockchain.height),
      (),
      SenderIsNotInPolicyRecipients(sender.stringRepr)
    )
}
