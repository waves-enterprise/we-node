package com.wavesenterprise.state.diffs

import cats.implicits._
import cats.syntax.either.catsSyntaxEitherId
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.{PermissionValidator, Permissions}
import com.wavesenterprise.block.BlockFeeCalculator.NgFee
import com.wavesenterprise.block.{Block, BlockFeeCalculator, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.database.snapshot.{ConsensualSnapshotSettings, EnabledSnapshot}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.metrics.Instrumented
import com.wavesenterprise.mining.MiningConstraint
import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.state._
import com.wavesenterprise.state.reader.CompositeBlockchain.composite
import com.wavesenterprise.transaction.ValidationError.ActivationError
import com.wavesenterprise.transaction.{Signed, Transaction, ValidationError}
import com.wavesenterprise.utils.ScorexLogging

import scala.concurrent.duration.FiniteDuration

object BlockDiffer extends ScorexLogging with Instrumented {

  type DiffResult = (Diff, Long, MiningConstraint)

  def fromBlock[Constraint <: MiningConstraint](blockchainSettings: BlockchainSettings,
                                                snapshotSettings: ConsensualSnapshotSettings,
                                                blockchain: Blockchain,
                                                permissionValidator: PermissionValidator,
                                                maybePrevBlock: Option[Block],
                                                block: Block,
                                                constraint: Constraint,
                                                txExpireTimeout: FiniteDuration,
                                                alreadyVerified: Boolean = false,
                                                alreadyVerifiedTxIds: Set[ByteStr] = Set.empty): Either[ValidationError, DiffResult] = {
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
        blockchainSettings = blockchainSettings,
        blockchain = blockchain,
        permissionValidator = permissionValidator,
        initConstraint = constraint,
        prevBlockTimestamp = maybePrevBlock.map(_.timestamp),
        blockGenerator = block.signerData.generatorAddress,
        prevBlockFeeDistr = prevBlockFeeDistr,
        currentBlockFeeDistr = currentBlockFeeDistr,
        timestamp = block.timestamp,
        txs = block.transactionData,
        currentBlockHeight = currentBlockHeight,
        block = block,
        alreadyVerified,
        alreadyVerifiedTxIds,
        txExpireTimeout
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

  def fromMicroBlock[Constraint <: MiningConstraint](
      blockchainSettings: BlockchainSettings,
      snapshotSettings: ConsensualSnapshotSettings,
      blockchain: Blockchain,
      permissionValidator: PermissionValidator,
      prevBlockTimestamp: Option[Long],
      micro: MicroBlock,
      timestamp: Long,
      constraint: Constraint,
      txExpireTimeout: FiniteDuration,
      alreadyVerified: Boolean = false,
      alreadyVerifiedTxIds: Set[ByteStr] = Set.empty): Either[ValidationError, (Diff, Long, Constraint)] = {
    for {
      // microblocks are processed within block which is next after 40-only-block which goes on top of activated height
      _ <- Either.cond(blockchain.activatedFeatures.contains(BlockchainFeature.NG.id), (), ActivationError(s"MicroBlocks are not yet activated"))
      txs = micro match {
        case txMicro: TxMicroBlock => txMicro.transactionData
        case _: VoteMicroBlock     => Seq.empty
      }
      _ <- checkSnapshotHeight(snapshotSettings, blockchain.height, txs)
      r <- apply(
        blockchainSettings = blockchainSettings,
        blockchain = blockchain,
        permissionValidator = permissionValidator,
        initConstraint = constraint,
        prevBlockTimestamp = prevBlockTimestamp,
        blockGenerator = micro.sender.toAddress,
        prevBlockFeeDistr = None,
        currentBlockFeeDistr = None,
        timestamp = timestamp,
        txs = txs,
        currentBlockHeight = blockchain.height,
        block = micro,
        alreadyVerified,
        alreadyVerifiedTxIds,
        txExpireTimeout
      )
    } yield r
  }

  private def apply[Constraint <: MiningConstraint](blockchainSettings: BlockchainSettings,
                                                    blockchain: Blockchain,
                                                    permissionValidator: PermissionValidator,
                                                    initConstraint: Constraint,
                                                    prevBlockTimestamp: Option[Long],
                                                    blockGenerator: Address,
                                                    prevBlockFeeDistr: Option[Portfolio],
                                                    currentBlockFeeDistr: Option[Map[Address, Portfolio]],
                                                    timestamp: Long,
                                                    txs: Seq[Transaction],
                                                    currentBlockHeight: Int,
                                                    block: Signed,
                                                    alreadyVerified: Boolean,
                                                    alreadyVerifiedTxIds: Set[ByteStr],
                                                    txExpireTimeout: FiniteDuration): Either[ValidationError, (Diff, Long, Constraint)] = {
    def updateConstraint(constraint: Constraint, blockchain: Blockchain, tx: Transaction): Constraint =
      constraint.put(blockchain, tx).asInstanceOf[Constraint]

    // Withdrawing from the sender of each transaction and crediting to the issuers of the assets
    val txDiffer = TransactionDiffer(
      settings = blockchainSettings,
      permissionValidator = permissionValidator,
      prevBlockTimestamp = prevBlockTimestamp,
      currentBlockTimestamp = timestamp,
      currentBlockHeight = currentBlockHeight,
      txExpireTimeout = txExpireTimeout,
      blockOpt = Some(block),
      minerOpt = Some(block.sender),
      alreadyVerified = alreadyVerified,
      alreadyVerifiedTxIds = alreadyVerifiedTxIds
    ).asFunc
    val ngIsActive = currentBlockFeeDistr.isEmpty
    val sponsorshipFeatureIsActive = blockchain
      .isFeatureActivated(BlockchainFeature.SponsoredFeesSupport, currentBlockHeight)

    val initPortfolios = currentBlockFeeDistr.orElse {
      prevBlockFeeDistr.map(portfolio => Map(blockGenerator -> portfolio))
    }.orEmpty
    // 60% reward for a miner only in case NG is activated. 100% reward for a miner and validators otherwise.
    val initMinerDiff = Diff.empty.copy(portfolios = initPortfolios)

    txs
      .foldLeft((initMinerDiff, 0L, initConstraint).asRight[ValidationError]) {
        case (error @ Left(_), _) => error
        case (Right((diffAcc, carryFeeAcc, constraintAcc)), tx) =>
          val updatedBlockchain = composite(blockchain, diffAcc)
          val updatedConstraint = updateConstraint(constraintAcc, updatedBlockchain, tx)

          if (updatedConstraint.isOverfilled) {
            Left(ValidationError.GenericError(s"Limit of txs was reached: '$initConstraint' -> '$updatedConstraint'"))
          } else {
            txDiffer(updatedBlockchain, tx).map { txDiff =>
              val updatedDiffAcc = diffAcc |+| txDiff

              if (ngIsActive) {
                val NgFee(nextBlockFee, portfolios) = BlockFeeCalculator.calcNgFee(updatedBlockchain, tx, sponsorshipFeatureIsActive, blockGenerator)
                // 100% reward for validators and 40% reward for miner
                val additionMinerDiff = Diff.empty.copy(portfolios = portfolios)
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

          (diff.copy(permissions = combinedPermissions), carry, constraint)
      }
  }
}
