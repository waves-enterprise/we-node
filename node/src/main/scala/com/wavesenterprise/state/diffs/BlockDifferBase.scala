package com.wavesenterprise.state.diffs

import cats.implicits._
import cats.syntax.either.catsSyntaxEitherId
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.Permissions
import com.wavesenterprise.block.BlockFeeCalculator.NgFee
import com.wavesenterprise.block.{Block, BlockFeeCalculator, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.database.snapshot.{ConsensualSnapshotSettings, EnabledSnapshot}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.metrics.Instrumented
import com.wavesenterprise.mining.{MiningConstraint, MultiDimensionalMiningConstraint}
import com.wavesenterprise.state._
import com.wavesenterprise.state.reader.CompositeBlockchain.composite
import com.wavesenterprise.transaction.ValidationError.ActivationError
import com.wavesenterprise.transaction.{Signed, Transaction, ValidationError}
import com.wavesenterprise.utils.ScorexLogging
import AssetHolder._

object BlockDifferBase {
  type BlockDiffResult      = (Diff, Long, MiningConstraint)
  type MicroBlockDiffResult = (Diff, Long, MultiDimensionalMiningConstraint)
}

trait BlockDifferBase extends ScorexLogging with Instrumented {

  import BlockDifferBase._

  protected def diffFromBlock(snapshotSettings: ConsensualSnapshotSettings,
                              blockchain: Blockchain,
                              maybePrevBlock: Option[Block],
                              block: Block,
                              constraint: MiningConstraint,
                              transactionDiffer: TransactionDiffer,
                              initMinerDiff: Diff = Diff.empty,
                              alreadyVerified: Boolean = false): Either[ValidationError, BlockDiffResult] = {
    val stateHeight = blockchain.height

    val ngHeightOpt             = blockchain.featureActivationHeight(BlockchainFeature.NG.id)
    val areSponsoredFeesEnabled = blockchain.isFeatureActivated(BlockchainFeature.SponsoredFeesSupport, stateHeight)

    /**
      * NG is actually activated one height after it's BlockchainFeature activation
      * That's why we're using `exists` and `forall` with non-equal compare
      */
    lazy val prevBlockFeeDistr: Option[Portfolio] =
      if (areSponsoredFeesEnabled)
        Some(Portfolio.empty.copy(balance = blockchain.carryFee))
      else if (ngHeightOpt.exists(ngHeight => stateHeight > ngHeight))
        maybePrevBlock.map(_.prevBlockFeePart())
      else None

    lazy val currentBlockFeeDistr: Option[Map[Address, Portfolio]] =
      if (ngHeightOpt.forall(ngHeight => stateHeight < ngHeight))
        Some(block.feesPortfolio())
      else
        None

    val currentBlockHeight = stateHeight + 1

    for {
      _ <- checkSnapshotHeight(snapshotSettings, currentBlockHeight, block.transactionData)
      _ <- if (alreadyVerified) Right(block) else Signed.validate(block)

      r <- apply(
        blockchain = blockchain,
        initConstraint = constraint,
        blockGenerator = block.signerData.generatorAddress,
        prevBlockFeeDistr = prevBlockFeeDistr,
        currentBlockFeeDistr = currentBlockFeeDistr,
        txs = block.transactionData,
        currentBlockHeight = currentBlockHeight,
        transactionDiffer = transactionDiffer,
        initMinerDiff = initMinerDiff
      )
    } yield r
  }

  private def checkSnapshotHeight(snapshotSettings: ConsensualSnapshotSettings,
                                  currentHeight: Int,
                                  txs: Seq[Transaction]): Either[ValidationError, Unit] =
    snapshotSettings match {
      case enabledSnapshot: EnabledSnapshot if txs.nonEmpty =>
        Either.cond(currentHeight < enabledSnapshot.snapshotHeight.value,
                    (),
                    ValidationError.ReachedSnapshotHeightError(enabledSnapshot.snapshotHeight.value))
      case _ =>
        Right(())
    }

  protected def diffFromMicroBlock(snapshotSettings: ConsensualSnapshotSettings,
                                   blockchain: Blockchain,
                                   micro: MicroBlock,
                                   constraint: MultiDimensionalMiningConstraint,
                                   transactionDiffer: TransactionDiffer,
                                   initMinerDiff: Diff = Diff.empty): Either[ValidationError, MicroBlockDiffResult] = {
    for {
      // microblocks are processed within block which is next after 40-only-block which goes on top of activated height
      _ <- Either.cond(blockchain.activatedFeatures.contains(BlockchainFeature.NG.id), (), ActivationError(s"MicroBlocks are not yet activated"))
      txs = micro match {
        case txMicro: TxMicroBlock => txMicro.transactionData
        case _: VoteMicroBlock     => Seq.empty
      }
      _ <- checkSnapshotHeight(snapshotSettings, blockchain.height, txs)
      diffResult <- apply(
        blockchain = blockchain,
        initConstraint = constraint,
        blockGenerator = micro.sender.toAddress,
        prevBlockFeeDistr = None,
        currentBlockFeeDistr = None,
        txs = txs,
        currentBlockHeight = blockchain.height,
        transactionDiffer = transactionDiffer,
        initMinerDiff = initMinerDiff,
      )
    } yield diffResult
  }

  protected def apply[Constraint <: MiningConstraint](
      blockchain: Blockchain,
      initConstraint: Constraint,
      blockGenerator: Address,
      prevBlockFeeDistr: Option[Portfolio],
      currentBlockFeeDistr: Option[Map[Address, Portfolio]],
      txs: Seq[Transaction],
      currentBlockHeight: Int,
      transactionDiffer: TransactionDiffer,
      initMinerDiff: Diff = Diff.empty,
  ): Either[ValidationError, (Diff, Long, Constraint)] = {
    def updateConstraint(constraint: Constraint, blockchain: Blockchain, tx: Transaction): Constraint =
      constraint.put(blockchain, tx).asInstanceOf[Constraint]

    val ngIsActive = currentBlockFeeDistr.isEmpty
    val sponsorshipFeatureIsActive = blockchain
      .isFeatureActivated(BlockchainFeature.SponsoredFeesSupport, currentBlockHeight)

    val initPortfolios = currentBlockFeeDistr.orElse {
      prevBlockFeeDistr.map(portfolio => Map(blockGenerator -> portfolio))
    }.orEmpty
    // 60% reward for a miner only in case NG is activated. 100% reward for a miner and validators otherwise.
    val initMinerDiffAfterAddingPortfolios = initMinerDiff.copy(portfolios = initPortfolios.toAssetHolderMap |+| initMinerDiff.portfolios)

    txs
      .foldLeft((initMinerDiffAfterAddingPortfolios, 0L, initConstraint).asRight[ValidationError]) {
        case (error @ Left(_), _) => error
        case (Right((diffAcc, carryFeeAcc, constraintAcc)), tx) =>
          val updatedBlockchain = composite(blockchain, diffAcc)
          val updatedConstraint = updateConstraint(constraintAcc, updatedBlockchain, tx)

          if (updatedConstraint.isOverfilled) {
            Left(ValidationError.GenericError(s"Limit of txs was reached: '$initConstraint' -> '$updatedConstraint'"))
          } else {
            transactionDiffer(updatedBlockchain, tx, None).map { txDiff =>
              val updatedDiffAcc = diffAcc |+| txDiff

              if (ngIsActive) {
                val NgFee(nextBlockFee, portfolios) =
                  BlockFeeCalculator.calcNgFee(updatedBlockchain, tx, sponsorshipFeatureIsActive, blockGenerator)
                // 100% reward for validators and 40% reward for miner
                val additionMinerDiff = Diff.empty.copy(portfolios = portfolios.toAssetHolderMap)
                val updatedDiff       = updatedDiffAcc |+| additionMinerDiff
                // carryFeeAcc + 60% of the current transaction fee
                val updatedCarry = carryFeeAcc + nextBlockFee
                (updatedDiff, updatedCarry, updatedConstraint)
              } else {
                (updatedDiffAcc, 0L, updatedConstraint)
              }
            }
          }
      }
      .map {
        case (diff, carry, constraint) =>
          val combinedPermissions: Map[Address, Permissions] = {
            diff.permissions.map {
              case (address, newPermissions) =>
                val oldPermissions = blockchain.permissions(address)
                address -> oldPermissions.combine(newPermissions)
            }
          }

          val newDiff = diff.copy(permissions = combinedPermissions)

          (newDiff, carry, constraint)
      }
  }
}
