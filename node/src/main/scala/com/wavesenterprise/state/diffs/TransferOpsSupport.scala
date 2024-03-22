package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.state.{AssetHolder, Blockchain, ContractId, Diff, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.docker.ExecutableTransaction
import com.wavesenterprise.transaction.docker.assets.{ContractAssetOperation, ContractTransferInV1}
import com.wavesenterprise.transaction.transfer.TransferTransaction
import com.wavesenterprise.transaction.{AssetId, ValidationError}

trait TransferOpsSupport {

  import com.wavesenterprise.state.AssetHolder._

  val NonExistentAssetError = GenericError(s"Attempt to transfer a nonexistent asset")

  protected def validateOverflow(tx: TransferTransaction): Either[ValidationError, Unit] = {
    Either
      .catchNonFatal(Math.addExact(tx.fee, tx.amount))
      .leftMap(_ => ValidationError.OverflowError)
      .void
  }

  protected def validateAssetExistence(blockchain: Blockchain, assetId: Option[AssetId], isFee: Boolean = false): Either[ValidationError, Unit] =
    Either.cond(
      assetId.isEmpty || blockchain.assetDescription(assetId.get).isDefined,
      (),
      GenericError(s"Unissued ${if (isFee) "fee "}asset id are not allowed")
    )

  protected def getPortfoliosMap(tx: TransferTransaction, sender: AssetHolder, recipient: AssetHolder): Map[AssetHolder, Portfolio] =
    tx.assetId match {
      case None =>
        Map(sender      -> Portfolio(-tx.amount, LeaseBalance.empty, Map.empty)) |+|
          Map(recipient -> Portfolio(tx.amount, LeaseBalance.empty, Map.empty))
      case Some(assetId) =>
        Map(sender      -> Portfolio(0, LeaseBalance.empty, Map(assetId -> -tx.amount))) |+|
          Map(recipient -> Portfolio(0, LeaseBalance.empty, Map(assetId -> tx.amount)))
    }

  protected def getPortfoliosMap(transferIn: ContractTransferInV1, sender: AssetHolder, recipient: AssetHolder): Map[AssetHolder, Portfolio] =
    transferIn.assetId match {
      case None =>
        Map(sender      -> Portfolio(-transferIn.amount, LeaseBalance.empty, Map.empty)) |+|
          Map(recipient -> Portfolio(transferIn.amount, LeaseBalance.empty, Map.empty))
      case Some(aid) =>
        Map(sender      -> Portfolio(0, LeaseBalance.empty, Map(aid -> -transferIn.amount))) |+|
          Map(recipient -> Portfolio(0, LeaseBalance.empty, Map(aid -> transferIn.amount)))
    }

  protected def getPortfoliosMap(transferOut: ContractAssetOperation.ContractTransferOutV1,
                                 sender: AssetHolder,
                                 recipient: AssetHolder): Map[AssetHolder, Portfolio] =
    transferOut.assetId match {
      case None =>
        Map(sender      -> Portfolio(-transferOut.amount, LeaseBalance.empty, Map.empty)) |+|
          Map(recipient -> Portfolio(transferOut.amount, LeaseBalance.empty, Map.empty))
      case Some(aid) =>
        Map(sender      -> Portfolio(0, LeaseBalance.empty, Map(aid -> -transferOut.amount))) |+|
          Map(recipient -> Portfolio(0, LeaseBalance.empty, Map(aid -> transferOut.amount)))
    }

  protected def getPortfoliosMap(payment: ContractAssetOperation.ContractPaymentV1,
                                 sender: AssetHolder,
                                 recipient: AssetHolder): Map[AssetHolder, Portfolio] =
    payment.assetId match {
      case None =>
        Map(sender      -> Portfolio(-payment.amount, LeaseBalance.empty, Map.empty)) |+|
          Map(recipient -> Portfolio(payment.amount, LeaseBalance.empty, Map.empty))
      case Some(aid) =>
        Map(sender      -> Portfolio(0, LeaseBalance.empty, Map(aid -> -payment.amount))) |+|
          Map(recipient -> Portfolio(0, LeaseBalance.empty, Map(aid -> payment.amount)))
    }

  protected def contractTransfersDiff(blockchain: Blockchain,
                                      tx: ExecutableTransaction,
                                      payments: List[ContractTransferInV1],
                                      height: Int): Either[ValidationError, Diff] = {

    def paymentToPortfoliosMap(tx: ExecutableTransaction, transferIn: ContractTransferInV1): Map[AssetHolder, Portfolio] = {
      val sender    = Address.fromPublicKey(tx.sender.publicKey).toAssetHolder
      val recipient = ContractId(tx.contractId).toAssetHolder
      getPortfoliosMap(transferIn, sender, recipient)
    }

    val allAssetsExist = payments.map(_.assetId).forall(_.forall(blockchain.assetDescription(_).isDefined))

    val combinedPortfolios =
      payments.foldLeft(Map.empty[AssetHolder, Portfolio])((result, transferIn) => result |+| paymentToPortfoliosMap(tx, transferIn))

    Either.cond(allAssetsExist, Diff(height, tx, combinedPortfolios), NonExistentAssetError)
  }
}
