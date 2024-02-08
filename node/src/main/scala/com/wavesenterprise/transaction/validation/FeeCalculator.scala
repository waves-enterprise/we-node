package com.wavesenterprise.transaction.validation

import cats.implicits._
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.settings.{FeeSettings, Fees, FunctionalitySettings}
import com.wavesenterprise.state.{AssetDescription, Blockchain, Sponsorship}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.docker.ExecutedContractTransaction
import com.wavesenterprise.transaction.transfer.MassTransferTransaction
import com.wavesenterprise.transaction.validation.FeeCalculator.{FeeHolder, FeeInNatives}

/**
  * Second reincarnation of Fee Calculator
  */
sealed trait FeeCalculator {
  def calculateMinFee(height: Int, tx: Transaction): Either[ValidationError, FeeHolder]
  def validateTxFee(height: Int, tx: Transaction): Either[ValidationError, Unit]
}

case object DisabledFeeCalculator extends FeeCalculator {
  def calculateMinFee(height: Int, tx: Transaction): Either[ValidationError, FeeHolder] =
    FeeInNatives(0L).asRight

  def validateTxFee(height: Int, tx: Transaction): Either[ValidationError, Unit] =
    Right(())
}

/**
  * Is used when node.blockchain.fees list is defined in Configuration
  */
case class EnabledFeeCalculator(blockchain: Blockchain, fs: FunctionalitySettings, feeSettings: Fees) extends FeeCalculator {
  import FeeCalculator._

  def isFeeSwitchActivated(height: Int): Boolean =
    blockchain
      .featureActivationHeight(BlockchainFeature.FeeSwitch.id)
      // [legacy] that's a thing to remember. Hopefully, will fix, but have to migrate pre-activated features state somehow then
      .exists(activationHeight => height >= activationHeight + fs.featureCheckBlocksPeriod)

  def areSponsoredFeesActivated(height: Int): Boolean =
    blockchain.isFeatureActivated(BlockchainFeature.SponsoredFeesSupport, height)

  def calculateMinFee(height: Int, tx: Transaction): Either[ValidationError, FeeHolder] = {
    if (zeroFeeTransactionTypes.contains(tx.builder.typeId)) {
      Right(FeeInNatives(0L))
    } else {
      if (areSponsoredFeesActivated(height)) {
        val minFeeInWest = baseFeeInWest(height, tx)
        tx.feeAssetId match {
          case None =>
            FeeInNatives(minFeeInWest).asRight

          case Some(assetId) =>
            for {
              assetInfo <- blockchain
                .assetDescription(assetId)
                .toRight(GenericError(s"Asset '$assetId' does not exist, cannot be used to pay fees"))

              westFee <- Either.cond(
                assetInfo.sponsorshipIsEnabled,
                minFeeInWest,
                GenericError(s"Asset '$assetId' is not sponsored, cannot be used to pay fees")
              )
            } yield FeeInAsset(assetId, assetInfo, westFee)
        }
      } else {
        // [legacy] pre-sponsorship era is basically deprecated, but present in a number of unit-tests
        val minFee = baseFeeInWest(height, tx)
        FeeInNatives(minFee).asRight
      }
    }
  }

  def validateTxFee(height: Int, tx: Transaction): Either[ValidationError, Unit] = {
    if (zeroFeeTransactionTypes.contains(tx.builder.typeId)) {
      Right(())
    } else {
      calculateMinFee(height, tx).flatMap {
        case FeeInNatives(minWestAmount) =>
          Either.cond(tx.fee >= minWestAmount, (), feeError("WEST", tx.builder.classTag.toString(), minWestAmount, tx.fee))

        case FeeInAsset(assetId, assetDescription, minWestAmount) =>
          val minAssetAmount = Sponsorship.fromWest(minWestAmount)
          Either.cond(assetDescription.sponsorshipIsEnabled,
            (),
            GenericError(s"Asset '$assetId' is not sponsored and thus cannot be used as a fee")) >>
            Either.cond(tx.fee >= minAssetAmount, (), feeError(assetId.toString, tx.builder.classTag.toString(), minAssetAmount, tx.fee))
      }
    }
  }

  /**
    * Returns minimum fee for given transactions
    * Additional fee is added for MassTransfer and Data transactions
    */
  private def baseFeeInWest(height: Int, tx: Transaction): Long = {
    val fees        = feeSettings.resolveActual(blockchain, height)
    val baseWestFee = fees.forTxType(tx.builder.typeId)

    val additionalFeeInUnits = tx match {
      case tx: MassTransferTransaction =>
        val additional = fees.forTxTypeAdditional(MassTransferTransaction.typeId)
        (tx.transfers.size + 1) / 2 * additional
      case tx: DataTransaction =>
        val base       = if (blockchain.isFeatureActivated(BlockchainFeature.SmartAccounts, height)) tx.bodyBytes() else tx.bytes()
        val additional = fees.forTxTypeAdditional(DataTransaction.typeId)
        (base.length - 1) / 1024 * additional
      case _ => 0
    }
    baseWestFee + additionalFeeInUnits
  }

  private def feeError(feeAssetStr: String, txClass: String, minExpected: Long, actualAmount: Long): GenericError =
    GenericError(s"Fee '$actualAmount' in '$feeAssetStr' for '$txClass' does not exceed minimal value of '$minExpected'")
}

object FeeCalculator {

  sealed trait FeeHolder { self =>
    val westAmount: Long
    def commitAdditions(addition: Long): FeeHolder
  }
  case class FeeInAsset(assetId: AssetId, assetDescription: AssetDescription, westAmount: Long) extends FeeHolder { self =>
    def commitAdditions(addition: Long): FeeInAsset = self.copy(westAmount = self.westAmount + addition)
  }
  case class FeeInNatives(westAmount: Long) extends FeeHolder { self =>
    def commitAdditions(addition: Long): FeeInNatives = self.copy(westAmount = self.westAmount + addition)
  }

  def apply(blockchain: Blockchain, fs: FunctionalitySettings, feeSettings: Fees): FeeCalculator = feeSettings match {
    case FeeSettings.FeesDisabled =>
      DisabledFeeCalculator
    case _ =>
      EnabledFeeCalculator(blockchain, fs, feeSettings)
  }

  /**
    * These transactions don't require fee
    */
  val zeroFeeTransactionTypes: Seq[Byte] = Seq(
    GenesisTransaction.typeId,
    GenesisPermitTransaction.typeId,
    GenesisRegisterNodeTransaction.typeId,
    ExecutedContractTransaction.typeId,
    AtomicTransaction.typeId
  )

  val additionalFeeTransactionTypes: Seq[Byte] = Seq(
    MassTransferTransaction.typeId,
    DataTransaction.typeId
  )
}
