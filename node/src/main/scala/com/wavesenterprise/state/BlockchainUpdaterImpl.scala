package com.wavesenterprise.state

import cats.implicits._
import cats.kernel.Semigroup
import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.{PermissionValidator, Permissions}
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, BlockHeader, BlockIdsCache, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.consensus._
import com.wavesenterprise.database.{PrivacyState, RollbackResult}
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.metrics.privacy.PrivacyMetrics
import com.wavesenterprise.metrics.{Instrumented, TxsInBlockchainStats}
import com.wavesenterprise.mining.{MiningConstraint, MiningConstraints, MultiDimensionalMiningConstraint}
import com.wavesenterprise.network.contracts.{ConfidentialDataId, ConfidentialDataType}
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PrivacyItemDescriptor}
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.appender.BaseAppender.BlockType
import com.wavesenterprise.state.appender.BaseAppender.BlockType.{Hard, Liquid}
import com.wavesenterprise.state.contracts.confidential.ConfidentialStateUpdater
import com.wavesenterprise.state.diffs.BlockDiffer
import com.wavesenterprise.state.diffs.BlockDifferBase.{BlockDiffResult, MicroBlockDiffResult}
import com.wavesenterprise.state.reader.{CompositeBlockchain, LeaseDetails}
import com.wavesenterprise.transaction.BlockchainEventError.{BlockAppendError, MicroBlockAppendError}
import com.wavesenterprise.transaction.ValidationError.{GenericError => ValidationGenericError}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransaction, ExecutedContractTransactionV4}
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.utils.pki.CrlData
import com.wavesenterprise.utils.{ScorexLogging, Time, UnsupportedFeature, forceStopApplication}
import com.wavesenterprise.{Schedulers, crypto}
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import java.security.cert.{Certificate, X509Certificate}
import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

class BlockchainUpdaterImpl(
    state: Blockchain with PrivacyState,
    settings: WESettings,
    time: Time,
    schedulers: Schedulers
) extends BlockchainUpdater
    with NG
    with ScorexLogging
    with MiningConstraintsHolder
    with Instrumented { self =>

  import BlockchainUpdaterImpl._
  import settings.blockchain.custom.functionality

  private[this] object locks {
    private[this] val readWriteLock = new ReentrantReadWriteLock

    def writeLock[B](f: => B): B = inLock(readWriteLock.writeLock(), f)

    def readLock[B](f: => B): B = inLock(readWriteLock.readLock(), f)

    private[this] def inLock[R](l: Lock, f: => R) = {
      try {
        l.lock()
        val res = f
        res
      } finally {
        l.unlock()
      }
    }
  }

  import locks._

  private val blockIdsCache = BlockIdsCache(settings.additionalCache.blockIds)

  // Implemented mutable to resolve circular dependency
  var maybeConfidentialStateUpdater: Option[ConfidentialStateUpdater] = None

  private lazy val maxBlockReadinessAge        = settings.miner.intervalAfterLastBlockThenGenerationIsAllowed.toMillis
  val permissionValidator: PermissionValidator = PermissionValidator(settings.blockchain.custom.genesis)

  override def ngState: Option[NgState] = innerNgState

  @volatile private var innerNgState: Option[NgState] = Option.empty

  @volatile private var innerCurrentMiner: Option[Address] = Option.empty

  @volatile private var innerRestTotalConstraint: MiningConstraint =
    MiningConstraints(state, state.height, settings.miner.maxBlockSizeInBytes).total

  override def restTotalConstraint: MiningConstraint = innerRestTotalConstraint

  private val internalLastBlockInfo = ConcurrentSubject.publish[LastBlockInfo](schedulers.blockchainUpdatesScheduler)

  override def isLastLiquidBlockId(id: ByteStr): Boolean = readLock {
    innerNgState.exists(_.contains(id)) || lastBlock.exists(_.uniqueId == id)
  }

  def isLastPersistenceBlock(id: ByteStr): Boolean = readLock {
    state.lastBlock.exists(_.uniqueId == id)
  }

  override val lastBlockInfo: Observable[LastBlockInfo] = internalLastBlockInfo.cache(1)
  lastBlockInfo.onErrorRestartUnlimited.subscribe()(schedulers.blockchainUpdatesScheduler) // Start caching

  private val internalPolicyUpdates                    = ConcurrentSubject.publish[PolicyUpdate](schedulers.blockchainUpdatesScheduler)
  override def policyUpdates: Observable[PolicyUpdate] = internalPolicyUpdates

  private val internalPolicyRollbacks                    = ConcurrentSubject.publish[PolicyDataId](schedulers.blockchainUpdatesScheduler)
  override def policyRollbacks: Observable[PolicyDataId] = internalPolicyRollbacks

  private val internalConfidentialDataUpdates                                      = ConcurrentSubject.publish[ConfidentialContractDataUpdate](schedulers.blockchainUpdatesScheduler)
  override def confidentialDataUpdates: Observable[ConfidentialContractDataUpdate] = internalConfidentialDataUpdates

  private val internalConfidentialDataRollbacks                          = ConcurrentSubject.publish[ConfidentialDataId](schedulers.blockchainUpdatesScheduler)
  override def confidentialDataRollbacks: Observable[ConfidentialDataId] = internalConfidentialDataRollbacks

  private val internalLastEvent: ConcurrentSubject[BlockchainEvent, BlockchainEvent] =
    ConcurrentSubject.publish[BlockchainEvent](monix.execution.Scheduler.global)

  override val lastBlockchainEvent: Observable[BlockchainEvent] = internalLastEvent.asyncBoundary(OverflowStrategy.Default)

  /* Unsafe. Used only inside locks */
  private[this] def blockchainReady: Boolean = {
    val lastBlock = innerNgState.map(_.base.timestamp).orElse(state.lastBlockTimestamp).get
    lastBlock + maxBlockReadinessAge > time.correctedTime()
  }

  // Store last block information in a cache
  lastBlockId.foreach { id =>
    internalLastBlockInfo.onNext(LastBlockInfo(id, height, score, blockchainReady))
  }

  private def displayFeatures(s: Set[Short]): String =
    s"FEATURE${if (s.size > 1) "S" else ""} ${s.mkString(", ")} ${if (s.size > 1) "have been" else "has been"}"

  private def featuresApprovedWithBlock(block: Block): Set[Short] = {
    val height = state.height + 1

    val featuresCheckPeriod        = functionality.featureCheckBlocksPeriod
    val blocksForFeatureActivation = functionality.blocksForFeatureActivation

    if (height % featuresCheckPeriod == 0) {
      val approvedFeatures = state
        .featureVotes(height)
        .map { case (feature, votes) => feature -> (if (block.featureVotes.contains(feature)) votes + 1 else votes) }
        .filter { case (_, votes) => votes >= blocksForFeatureActivation }
        .keySet

      if (approvedFeatures.nonEmpty) log.info(s"${displayFeatures(approvedFeatures)} APPROVED at height $height")

      val unimplementedApproved = approvedFeatures.diff(BlockchainFeature.implemented)
      if (unimplementedApproved.nonEmpty) {
        log.warn(s"UNIMPLEMENTED ${displayFeatures(unimplementedApproved)} APPROVED ON BLOCKCHAIN")
        log.warn("PLEASE, UPDATE THE NODE AS SOON AS POSSIBLE")
        log.warn("OTHERWISE THE NODE WILL BE STOPPED OR FORKED UPON FEATURE ACTIVATION")
      }

      val activatedFeatures: Set[Short] = state.activatedFeaturesAt(height)

      val unimplementedActivated = activatedFeatures.diff(BlockchainFeature.implemented)
      if (unimplementedActivated.nonEmpty) {
        log.error(s"UNIMPLEMENTED ${displayFeatures(unimplementedActivated)} ACTIVATED ON BLOCKCHAIN")
        log.error("PLEASE, UPDATE THE NODE IMMEDIATELY")
        if (settings.features.autoShutdownOnUnsupportedFeature) {
          log.error("FOR THIS REASON THE NODE WAS STOPPED AUTOMATICALLY")
          forceStopApplication(UnsupportedFeature)
        } else log.error("OTHERWISE THE NODE WILL END UP ON A FORK")
      }

      approvedFeatures
    } else {

      Set.empty
    }
  }

  override def processBlock(block: Block,
                            postAction: ConsensusPostAction,
                            blockType: BlockType,
                            isOwn: Boolean,
                            alreadyVerifiedTxIds: Set[ByteStr],
                            certChainStore: CertChainStore): Either[ValidationError, Option[DiscardedTransactions]] =
    writeLock {
      val height                             = state.height
      val notImplementedFeatures: Set[Short] = state.activatedFeaturesAt(height).diff(BlockchainFeature.implemented)

      def checkFeaturesImplementation: Either[ValidationError, Unit] =
        Either
          .cond(
            !settings.features.autoShutdownOnUnsupportedFeature || notImplementedFeatures.isEmpty,
            (),
            ValidationGenericError {
              s"UNIMPLEMENTED '${displayFeatures(notImplementedFeatures)}' ACTIVATED ON BLOCKCHAIN, UPDATE THE NODE IMMEDIATELY"
            }
          )

      checkFeaturesImplementation >>
        innerNgState
          .map(ng => processBlockWithNg(block, ng, isOwn, alreadyVerifiedTxIds, certChainStore))
          .getOrElse(processBlockWithoutNg(block, isOwn, alreadyVerifiedTxIds, certChainStore))
          .map { maybeDiffResultWithDiscardedTxs =>
            maybeDiffResultWithDiscardedTxs.map {
              case ((newBlockDiff, carry, updatedTotalConstraint), discardedTxs) =>
                val height = state.height + 1
                innerRestTotalConstraint = updatedTotalConstraint
                val consensusPostActionDiff = postAction(this)
                val newNgState              = new NgState(block, blockType, newBlockDiff, carry, 0, featuresApprovedWithBlock(block), consensusPostActionDiff)
                innerNgState = Some(newNgState)
                innerCurrentMiner = Some(newNgState.base.signerData.generatorAddress)
                lastBlockId.foreach(id => internalLastBlockInfo.onNext(LastBlockInfo(id, height, score, blockchainReady)))

                val generationIsAllowed = block.timestamp > time.getTimestamp() - maxBlockReadinessAge
                if (generationIsAllowed || (height % 100 == 0)) log.info(s"New height: '$height'")

                blockIdsCache.put(block.uniqueId)

                discardedTxs
            }
          }
    }

  private def processBlockWithNg(
      block: Block,
      ng: NgState,
      isOwn: Boolean,
      alreadyVerifiedTxIds: Set[ByteStr],
      certChainStore: CertChainStore
  ): Either[ValidationError, Option[(BlockDiffResult, DiscardedTransactions)]] = {
    if (ng.base.reference == block.reference) {
      processBlockWithSameParent(block, ng, isOwn, alreadyVerifiedTxIds, certChainStore)
    } else {
      processBlock(block, ng, isOwn, alreadyVerifiedTxIds, certChainStore).map(Some(_))
    }
  }

  protected def checkBlockSenderCertChain(sender: PublicKeyAccount, timestamp: Long): Either[ValidationError, Unit] = Right(())

  protected def addToConfidentialTransactions(tx: Transaction): Unit = confidentialContractInfoAndExTx(tx) match {
    case Some((contractInfo, exTx)) if nodeIsContractValidator() && contractInfo.groupParticipants.contains(settings.ownerAddress) =>
      internalConfidentialDataUpdates.onNext {
        log.trace(s"Confidential tx '${tx.id()}' has been added to the stream from blockchain")
        ConfidentialContractDataUpdate(
          ConfidentialDataId(ContractId(contractInfo.contractId), exTx.outputCommitment, ConfidentialDataType.Output),
          tx.timestamp.some
        )
      }
    case _ => ()
  }

  private def nodeIsContractValidator(): Boolean = this.lastBlockContractValidators.contains(settings.ownerAddress)

  private def confidentialContractInfoAndExTx(tx: Transaction): Option[(ContractInfo, ExecutedContractTransactionV4)] = tx match {
    case executableTx: ExecutedContractTransactionV4 => this.contract(ContractId(executableTx.tx.contractId)).map(_ -> executableTx)
    case _                                           => None
  }

  private def processBlock(
      block: Block,
      ng: NgState,
      isOwn: Boolean,
      alreadyVerifiedTxIds: Set[ByteStr],
      certChainStore: CertChainStore
  ): Either[ValidationError, (BlockDiffResult, Seq[Transaction])] = {
    log.trace(s"Processing block '${block.uniqueId}'")
    // Forge block and diff from NG state. The new block refers to the liquid block we need to get from NG.
    // The new block may not refer to the last added micro-block, so some micro-blocks from the end may be discarded.
    measureSuccessful(forgeBlockTimeStats, ng.totalDiffOf(block.reference)) match {
      case None =>
        Left(BlockAppendError(s"References incorrect or non-existing block", block))
      case Some((forgedLiquidBlock, forgedLiquidDiff, carry, _, discardedMicroBlocks)) =>
        if (isOwn || forgedLiquidBlock.signatureValid()) {
          checkBlockSenderCertChain(block.sender, block.timestamp) >> {
            if (discardedMicroBlocks.nonEmpty) {
              microBlockForkStats.increment()
              microBlockForkHeightStats.record(discardedMicroBlocks.size)
            }

            val blockchainWithNewLiquidBlock: Blockchain =
              CompositeBlockchain.composite(state, forgedLiquidDiff, carry, new ContractValidatorsProvider(this))

            val constraint = {
              val height            = state.heightOf(forgedLiquidBlock.reference).getOrElse(0)
              val miningConstraints = MiningConstraints(state, height, settings.miner.maxBlockSizeInBytes)
              miningConstraints.total
            }

            // We need to calculate diff between the new block and the state with added liquid block,
            // because the new block might include some data, so we are going to put that data into NG later.
            buildBlockDiff(
              blockchain = blockchainWithNewLiquidBlock,
              block = block,
              maybePrevBlock = Some(forgedLiquidBlock),
              constraint = constraint,
              certChainStore = certChainStore,
              isOwn = isOwn,
              alreadyVerifiedTxIds = alreadyVerifiedTxIds
            ).map { nextNgState =>
              val newHeight =
                state.append(forgedLiquidDiff, carry, forgedLiquidBlock, ng.consensusPostActionDiff, forgedLiquidDiff.certByDnHash.values.toSet)

              maybeConfidentialStateUpdater.foreach(_.processBlock(forgedLiquidBlock, newHeight, block))

              TxsInBlockchainStats.record(ng.transactions.size)

              val baseBlockIsLiquid = innerNgState.exists(_.baseBlockType == Liquid)

              val broadcastEvent =
                if (baseBlockIsLiquid) {
                  BlockAppended(forgedLiquidBlock, newHeight)
                } else {
                  processPolicyDiff(forgedLiquidDiff, settings.ownerAddress)
                  AppendedBlockHistory(forgedLiquidBlock, newHeight)
                }

              internalLastEvent.onNext(broadcastEvent)

              // This fix changes validation for some cases, that's why it is enabled via soft-fork
              if (
                state.isFeatureActivated(BlockchainFeature.PoaOptimisationFix, height) ||
                state.isFeatureActivated(BlockchainFeature.SponsoredFeesSupport, height)
              ) {
                innerNgState = None
                innerCurrentMiner = None
              }

              val discardedTxs = discardedMicroBlocks.flatMap {
                case txMicro: TxMicroBlock => txMicro.transactionData
                case _: VoteMicroBlock     => Seq.empty
              }

              discardedTxs.foreach {
                case tx: PolicyDataHashTransaction =>
                  internalPolicyRollbacks.onNext(PolicyDataId(tx.policyId, tx.dataHash))
                case _ => ()
              }

              nextNgState -> discardedTxs
            }
          }
        } else {
          val errorText = s"Forged block has invalid signature: base '${ng.base}', requested reference '${block.reference}'"
          log.error(errorText)
          Left(BlockAppendError(errorText, block))
        }
    }
  }

  protected def buildBlockDiff(blockchain: Blockchain,
                               block: Block,
                               maybePrevBlock: Option[Block],
                               constraint: MiningConstraint,
                               certChainStore: CertChainStore,
                               isOwn: Boolean,
                               alreadyVerifiedTxIds: Set[ByteStr]): Either[ValidationError, BlockDiffResult] =
    BlockDiffer
      .fromBlock(
        blockchainSettings = settings.blockchain,
        snapshotSettings = settings.consensualSnapshot,
        blockchain = blockchain,
        permissionValidator = permissionValidator,
        maybePrevBlock = maybePrevBlock,
        block = block,
        constraint = constraint,
        txExpireTimeout = settings.utx.txExpireTimeout,
        alreadyVerified = isOwn,
        alreadyVerifiedTxIds = alreadyVerifiedTxIds
      )

  protected def buildMicroBlockDiff(microBlock: MicroBlock,
                                    ng: NgState,
                                    constraint: MultiDimensionalMiningConstraint,
                                    isOwn: Boolean,
                                    alreadyVerifiedTxIds: Set[ByteStr],
                                    certChainStore: CertChainStore): Either[ValidationError, MicroBlockDiffResult] =
    BlockDiffer.fromMicroBlock(
      blockchainSettings = settings.blockchain,
      snapshotSettings = settings.consensualSnapshot,
      blockchain = this,
      permissionValidator = permissionValidator,
      prevBlockTimestamp = state.lastBlockTimestamp,
      micro = microBlock,
      timestamp = ng.base.timestamp,
      constraint = constraint,
      txExpireTimeout = settings.utx.txExpireTimeout,
      alreadyVerified = isOwn,
      alreadyVerifiedTxIds = alreadyVerifiedTxIds
    )

  private def processBlockWithSameParent(
      block: Block,
      ng: NgState,
      isOwn: Boolean,
      alreadyVerifiedTxIds: Set[ByteStr],
      certChainStore: CertChainStore): Either[ValidationError, Option[(BlockDiffResult, DiscardedTransactions)]] = {
    log.debug(s"Processing liquid block '${block.uniqueId}'")
    if (block.blockScore() > ng.base.blockScore()) {
      val height            = state.unsafeHeightOf(ng.base.reference)
      val miningConstraints = MiningConstraints(state, height, settings.miner.maxBlockSizeInBytes)

      buildBlockDiff(
        blockchain = state,
        block = block,
        maybePrevBlock = state.lastBlock,
        constraint = miningConstraints.total,
        certChainStore = certChainStore,
        isOwn = isOwn,
        alreadyVerifiedTxIds = alreadyVerifiedTxIds
      ).map { diffResult =>
        log.trace(
          s"Better liquid block with score '${block.blockScore()}' received and applied instead of existing with score '${ng.base.blockScore()}'")
        Some(diffResult -> ng.transactions)
      }
    } else if (areVersionsOfSameBlock(block, ng.base)) {
      if (block.transactionData.lengthCompare(ng.transactions.size) <= 0) {
        log.trace(s"Existing liquid block is the same version of received one and has no less transactions, discarding received '$block'")
        Right(None)
      } else {
        log.trace(s"New liquid block is same version of received one and has more transactions, swapping")
        val height            = state.unsafeHeightOf(ng.base.reference)
        val miningConstraints = MiningConstraints(state, height, settings.miner.maxBlockSizeInBytes)

        buildBlockDiff(
          blockchain = state,
          block = block,
          maybePrevBlock = state.lastBlock,
          constraint = miningConstraints.total,
          certChainStore = certChainStore,
          isOwn = isOwn,
          alreadyVerifiedTxIds = alreadyVerifiedTxIds
        ).map(diffResult => Some(diffResult -> Seq.empty[Transaction]))
      }
    } else {
      Left(
        BlockAppendError(
          s"Competitors liquid block '$block' with score '${block.blockScore()}' is not better than existing '${ng.base}' with score '${ng.base.blockScore()}'",
          block
        ))
    }
  }

  private def processBlockWithoutNg(block: Block,
                                    isOwn: Boolean,
                                    alreadyVerifiedTxIds: Set[ByteStr],
                                    certChainStore: CertChainStore): Either[ValidationError, Some[(BlockDiffResult, DiscardedTransactions)]] = {
    log.debug(s"Processing block '${block.uniqueId}' without NG")
    state.lastBlockId match {
      case Some(lastBlockId) if lastBlockId != block.reference =>
        val blockDetails = if (state.contains(block.reference)) "exits, but not last persisted" else "doesn't exist"
        Left(BlockAppendError(s"Block reference '${block.reference}' " + blockDetails, block))
      case maybeLastBlockId =>
        val height            = maybeLastBlockId.fold(0)(state.unsafeHeightOf)
        val miningConstraints = MiningConstraints(state, height, settings.miner.maxBlockSizeInBytes)

        buildBlockDiff(
          blockchain = state,
          block = block,
          maybePrevBlock = state.lastBlock,
          constraint = miningConstraints.total,
          certChainStore = certChainStore,
          isOwn = isOwn,
          alreadyVerifiedTxIds = alreadyVerifiedTxIds
        ).map { diffResult =>
          Some(diffResult -> Seq.empty[Transaction])
        }
    }
  }

  private def processPolicyDiff(diff: Diff, ownerAddress: Address): Unit = {

    case class DataHashWithTs(dataHash: PolicyDataHash, timestamp: Long)

    val dataUpdatesWithTs = diff.policiesDataHashes.collect {
      case (policyId, dataHashWithId) if policyRecipients(policyId).contains(ownerAddress) =>
        policyId -> dataHashWithId.map(tx => DataHashWithTs(tx.dataHash, tx.timestamp))
    }

    val timestampByDataId = dataUpdatesWithTs.flatMap {
      case (policyId, hashesWithTs) =>
        hashesWithTs.view.map { hashWithTs =>
          PolicyDataId(policyId, hashWithTs.dataHash) -> hashWithTs.timestamp
        }
    }

    val fromDataUpdate = dataUpdatesWithTs.mapValues(_.map(_.dataHash))

    val fromRecipientsUpdate = for {
      (policyId, policyDiff: PolicyDiffValue) <- diff.policies
      if policyDiff.recipientsToAdd.contains(ownerAddress)
    } yield policyId -> policyDataHashes(policyId)

    val policyDataIds = Semigroup
      .combine(fromDataUpdate, fromRecipientsUpdate)
      .flatMap {
        case (policyId, dataHashes) => dataHashes.map(PolicyDataId(policyId, _))
      }
      .toSet

    if (policyDataIds.nonEmpty) {
      val added = state.addToPending(policyDataIds)
      PrivacyMetrics.pendingSizeStats.increment(added)
    }

    policyDataIds.foreach { pdi =>
      internalPolicyUpdates.onNext(PolicyUpdate(pdi, timestampByDataId.get(pdi)))
    }
  }

  override def removeAfter(blockId: ByteStr): Either[ValidationError, Seq[Block]] = writeLock {
    log.info(s"Removing blocks after '${blockId.trim}' from blockchain")

    val ng = innerNgState
    if (ng.exists(_.contains(blockId))) {
      log.trace(s"NG contains block '$blockId', no rollback is necessary")
      Right(Seq.empty)
    } else {
      val discardedNgBlock = ng.map(_.bestLiquidBlock).toSeq
      innerNgState = None
      innerCurrentMiner = None
      maybeConfidentialStateUpdater.foreach(_.discardLiquidState())

      state
        .rollbackTo(blockId)
        .map { case RollbackResult(initialHeight, discardedPersistenceBlocks) =>
          val allDiscardedBlocks = discardedPersistenceBlocks ++ discardedNgBlock

          val allDiscardedTxs = allDiscardedBlocks.flatMap { block =>
            block.transactionData
              .collect {
                case pdh: PolicyDataHashTransaction => List(pdh)
                case atomixTx: AtomicTransaction =>
                  atomixTx.transactions.collect {
                    case pdh: PolicyDataHashTransactionV3 => pdh
                  }
              }
              .flatten
              .foreach { tx =>
                val (isRemovedFromPending, _) = state.removeFromPendingAndLost(tx.policyId, tx.dataHash)
                if (isRemovedFromPending) PrivacyMetrics.pendingSizeStats.decrement()
                internalPolicyRollbacks.onNext(PolicyDataId(tx.policyId, tx.dataHash))
              }

            blockIdsCache.invalidate(block.uniqueId)
            block.transactionData
          }

          internalLastEvent.onNext(RollbackCompleted(blockId, allDiscardedTxs))

          maybeConfidentialStateUpdater.foreach(_.rollbackPersistenceBlocks(discardedPersistenceBlocks.reverse, initialHeight))

          allDiscardedBlocks
        }.leftMap(err => ValidationGenericError(err))
    }
  }

  private def requestConfidentialOutput(microBlock: MicroBlock): Unit = {
    microBlock match {
      case microBlockWithTx: TxMicroBlock =>
        microBlockWithTx.transactionData.foreach(addToConfidentialTransactions)
      case _ =>
    }
  }

  override def processMicroBlock(microBlock: MicroBlock,
                                 isOwn: Boolean,
                                 alreadyVerifiedTxIds: Set[ByteStr],
                                 certChainStore: CertChainStore): Either[ValidationError, Unit] =
    writeLock {
      innerNgState match {
        case None =>
          Left(MicroBlockAppendError("No base block exists", microBlock))

        case Some(ng) if ng.base.signerData.generatorAddress != microBlock.sender.toAddress =>
          Left(MicroBlockAppendError("Base block has been generated by another account", microBlock))

        case Some(ng) =>
          ng.lastMicroBlock match {
            case None if ng.base.uniqueId != microBlock.prevLiquidBlockSig =>
              blockMicroForkStats.increment()
              Left(MicroBlockAppendError("It's first micro and it doesn't reference base block (which exists)", microBlock))

            case Some(prevMicro) if prevMicro.totalLiquidBlockSig != microBlock.prevLiquidBlockSig =>
              microMicroForkStats.increment()
              Left(MicroBlockAppendError("It doesn't reference last known micro-block (which exists)", microBlock))

            case _ =>
              for {
                _ <- checkMicroBlockSignatures(ng, microBlock, isOwn)
                _ = requestConfidentialOutput(microBlock: MicroBlock)
                diffResult <- {
                  val constraints  = MiningConstraints(state, state.height, settings.miner.maxBlockSizeInBytes)
                  val mdConstraint = MultiDimensionalMiningConstraint(innerRestTotalConstraint, constraints.micro)
                  buildMicroBlockDiff(microBlock = microBlock,
                                      ng = ng,
                                      constraint = mdConstraint,
                                      isOwn = isOwn,
                                      alreadyVerifiedTxIds = alreadyVerifiedTxIds,
                                      certChainStore = certChainStore)
                }
              } yield {
                val (diff, carry, updatedMdConstraint) = diffResult
                innerRestTotalConstraint = updatedMdConstraint.constraints.head
                ng.append(microBlock, diff, carry, 0L, certChainStore, diff.usedCrlHashes)
                internalLastBlockInfo.onNext(LastBlockInfo(microBlock.totalLiquidBlockSig, height, score, ready = true))

                microBlock match {
                  case txMicro: TxMicroBlock => internalLastEvent.onNext(MicroBlockAppended(txMicro.transactionData))
                  case _: VoteMicroBlock     => ()
                }

                processPolicyDiff(diff, settings.ownerAddress)

              }
          }
      }
    }

  private def checkMicroBlockSignatures(ng: NgState, microBlock: MicroBlock, isOwn: Boolean): Either[ValidationError, Unit] =
    if (isOwn) {
      Right(())
    } else {
      for {
        _ <- Signed.validate(microBlock)
        prevLiquidBlock <- ng
          .liquidBlock(microBlock.prevLiquidBlockSig)
          .toRight(MicroBlockAppendError(s"Referenced liquid block '${microBlock.prevLiquidBlockSig}' does not exist", microBlock))
        newLiquidBlock <- microBlock match {
          case txMicro: TxMicroBlock =>
            Right(prevLiquidBlock.copy(transactionData = prevLiquidBlock.transactionData ++ txMicro.transactionData))
          case voteMicro: VoteMicroBlock =>
            prevLiquidBlock.blockHeader.consensusData.asCftMaybe().map { prevConsensusData =>
              val newHeader = prevLiquidBlock.blockHeader.copy(consensusData = prevConsensusData.copy(votes = voteMicro.votes))
              prevLiquidBlock.copy(blockHeader = newHeader)
            }
        }
        _ <- Either.cond(
          crypto.verify(microBlock.totalLiquidBlockSig.arr, newLiquidBlock.bytesWithoutSignature(), microBlock.sender.publicKey),
          (),
          MicroBlockAppendError(s"Invalid liquid block total signature '${microBlock.totalLiquidBlockSig}'", microBlock)
        )
      } yield ()
    }

  def isRecentlyApplied(id: ByteStr): Boolean = blockIdsCache.contains(id)

  def shutdown(): Unit = {
    internalLastBlockInfo.onComplete()
    internalPolicyUpdates.onComplete()
    internalLastEvent.onComplete()
  }

  /* Unsafe. Used only inside locks */
  private def newlyApprovedFeatures = innerNgState.fold(Map.empty[Short, Int])(_.approvedFeatures.map(_ -> height).toMap)

  override def approvedFeatures: Map[Short, Int] = readLock {
    newlyApprovedFeatures ++ state.approvedFeatures
  }

  override def activatedFeatures: Map[Short, Int] = readLock {
    newlyApprovedFeatures.mapValues(_ + functionality.featureCheckBlocksPeriod) ++ state.activatedFeatures
  }

  override def featureVotes(height: Int): Map[Short, Int] = readLock {
    val innerVotes = state.featureVotes(height)
    innerNgState match {
      case Some(ng) if this.height <= height =>
        val ngVotes = ng.base.featureVotes.map { featureId =>
          featureId -> (innerVotes.getOrElse(featureId, 0) + 1)
        }.toMap

        innerVotes ++ ngVotes
      case _ => innerVotes
    }
  }

  /* Unsafe. Used only inside locks */
  private def liquidBlock(): Option[Block] = innerNgState.map(_.bestLiquidBlock)

  override def blockHeaderAndSize(blockId: BlockId): Option[(BlockHeader, Int)] = readLock {
    liquidBlock().filter(_.uniqueId == blockId).map(t => (t.blockHeader, t.bytes().length)) orElse state.blockHeaderAndSize(blockId)
  }

  override def height: Int = readLock {
    state.height + innerNgState.fold(0)(_ => 1)
  }

  override def blockBytes(height: Int): Option[Array[Byte]] = readLock {
    state
      .blockBytes(height)
      .orElse(innerNgState.collect { case ng if height == state.height + 1 => ng.bestLiquidBlock.bytes() })
  }

  override def scoreOf(blockId: BlockId): Option[BigInt] = readLock {
    state
      .scoreOf(blockId)
      .orElse(innerNgState.collect { case ng if ng.contains(blockId) => state.score + ng.base.blockScore() })
  }

  override def heightOf(blockId: BlockId): Option[Int] = readLock {
    state
      .heightOf(blockId)
      .orElse(innerNgState.collect { case ng if ng.contains(blockId) => this.height })
  }

  override def lastBlockIds(howMany: Int): Seq[BlockId] = readLock {
    innerNgState.fold(state.lastBlockIds(howMany))(_.bestLiquidBlockId +: state.lastBlockIds(howMany - 1))
  }

  override def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]] = readLock {
    innerNgState.fold(state.lastBlockIds(startBlock, howMany)) { ng =>
      if (ng.contains(startBlock))
        Some(startBlock +: state.lastBlockIds(howMany - 1))
      else {
        state.lastBlockIds(startBlock, howMany)
      }
    }
  }

  override def microBlock(id: BlockId): Option[MicroBlock] = readLock {
    for {
      ng <- innerNgState
      mb <- ng.microBlock(id)
    } yield mb
  }

  def lastBlockTimestamp: Option[Long] = readLock {
    innerNgState.map(_.base.timestamp).orElse(state.lastBlockTimestamp)
  }

  def lastBlockId: Option[ByteStr] = readLock {
    innerNgState.map(_.bestLiquidBlockId).orElse(state.lastBlockId)
  }

  def blockAt(height: Int): Option[Block] = readLock {
    if (height == this.height)
      innerNgState.map(_.bestLiquidBlock)
    else
      state.blockAt(height)
  }

  override def lastPersistedBlockIds(count: Int): Seq[BlockId] = readLock {
    state.lastBlockIds(count)
  }

  override def microblockIds: Seq[BlockId] = readLock {
    innerNgState.fold(Seq.empty[BlockId])(_.microBlockIds)
  }

  override def bestLastBlockInfo(maxTimestamp: Long): Option[BlockMinerInfo] = readLock {
    innerNgState
      .map(_.bestLastBlockInfo(maxTimestamp))
      .orElse(state.lastBlock.map(b => BlockMinerInfo(b.consensusData, b.timestamp, b.uniqueId)))
  }

  override def score: BigInt = readLock {
    state.score + innerNgState.fold(BigInt(0))(_.bestLiquidBlock.blockScore())
  }

  override def lastBlock: Option[Block] = readLock {
    innerNgState.map(_.bestLiquidBlock).orElse(state.lastBlock)
  }

  override def lastPersistenceBlock: Option[Block] = readLock {
    state.lastPersistenceBlock
  }

  override def carryFee: Long = readLock {
    innerNgState.map(_.carryFee).getOrElse(state.carryFee)
  }

  override def blockBytes(blockId: ByteStr): Option[Array[Byte]] = readLock {
    (for {
      ng                  <- innerNgState
      (block, _, _, _, _) <- ng.totalDiffOf(blockId)
    } yield block.bytes()).orElse(state.blockBytes(blockId))
  }

  override def blockHeaderByIdWithLiquidVariations(blockId: BlockId): Option[BlockHeader] = readLock {
    (for {
      ng                  <- innerNgState
      (block, _, _, _, _) <- ng.totalDiffOf(blockId)
    } yield block.blockHeader).orElse {
      state
        .blockHeaderAndSize(blockId)
        .map { case (header, _) => header }
    }
  }

  override def liquidBlockById(blockId: BlockId): Option[Block] = readLock {
    for {
      ng                  <- innerNgState
      (block, _, _, _, _) <- ng.totalDiffOf(blockId)
    } yield block
  }

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] = readLock {
    innerNgState match {
      case Some(ng) if ng.contains(parentSignature) => Some(Seq.empty[ByteStr])
      case maybeNg =>
        state.blockIdsAfter(parentSignature, howMany).map { ib =>
          if (ib.lengthCompare(howMany) < 0) ib ++ maybeNg.map(_.bestLiquidBlockId) else ib
        }
    }
  }

  override def parent(block: Block, back: Int): Option[Block] = readLock {
    innerNgState match {
      case Some(ng) if ng.contains(block.reference) =>
        if (back == 1) Some(ng.base) else state.parent(ng.base, back - 1)
      case _ =>
        state.parent(block, back)
    }
  }

  override def parentHeader(block: Block): Option[BlockHeader] = readLock {
    innerNgState match {
      case Some(ng) if ng.contains(block.reference) =>
        Some(ng.base.blockHeader)
      case _ =>
        state.parentHeader(block)
    }
  }

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = readLock {
    if (height == state.height + 1)
      innerNgState.map(x => (x.bestLiquidBlock.blockHeader, x.bestLiquidBlock.bytes().length))
    else
      state.blockHeaderAndSize(height)
  }

  override def addressPortfolio(a: Address): Portfolio = readLock {
    val p = innerNgState.fold(Portfolio.empty)(_.bestLiquidDiff.portfolios.getOrElse(a.toAssetHolder, Portfolio.empty))
    state.addressPortfolio(a).combine(p)
  }

  override def contractPortfolio(contractId: ContractId): Portfolio = readLock {
    val p = innerNgState.fold(Portfolio.empty)(_.bestLiquidDiff.portfolios.getOrElse(contractId.toAssetHolder, Portfolio.empty))
    state.contractPortfolio(contractId).combine(p)
  }

  override def transactionInfo(id: AssetId): Option[(Int, Transaction)] = readLock {
    innerNgState
      .fold(Diff.empty)(_.bestLiquidDiff)
      .transactionsMap
      .get(id)
      .map(t => (t._1, t._2))
      .orElse(state.transactionInfo(id))
  }

  override def addressTransactions(address: Address,
                                   types: Set[Transaction.Type],
                                   count: Int,
                                   fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]] =
    readLock {
      addressTransactionsFromStateAndDiff(state, innerNgState.map(_.bestLiquidDiff))(address, types, count, fromId)
    }

  override def containsTransaction(tx: Transaction): Boolean =
    readLock(innerNgState.fold(state.containsTransaction(tx)) { ng =>
      ng.bestLiquidDiff.transactionsMap.contains(tx.id()) || state.containsTransaction(tx)
    })

  override def containsTransaction(id: ByteStr): Boolean = readLock {
    innerNgState.fold(state.containsTransaction(id)) { ng =>
      ng.bestLiquidDiff.transactionsMap.contains(id) || state.containsTransaction(id)
    }
  }

  override def assets(): Set[AssetId] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).assets()
  }

  override def asset(id: AssetId): Option[AssetInfo] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).asset(id)
  }

  override def assetDescription(id: AssetId): Option[AssetDescription] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).assetDescription(id)
  }

  override def resolveAlias(alias: Alias): Either[ValidationError, Address] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).resolveAlias(alias)
  }

  override def addressLeaseBalance(address: Address): LeaseBalance =
    readLock(innerNgState match {
      case Some(ng) =>
        cats.Monoid.combine(state.addressLeaseBalance(address), ng.bestLiquidDiff.portfolios.getOrElse(address.toAssetHolder, Portfolio.empty).lease)
      case None =>
        state.addressLeaseBalance(address)
    })

  override def contractLeaseBalance(contractId: ContractId): LeaseBalance =
    readLock(innerNgState match {
      case Some(ng) =>
        cats.Monoid.combine(state.contractLeaseBalance(contractId),
                            ng.bestLiquidDiff.portfolios.getOrElse(contractId.toAssetHolder, Portfolio.empty).lease)
      case None =>
        state.contractLeaseBalance(contractId)
    })

  override def leaseDetails(leaseId: LeaseId): Option[LeaseDetails] =
    readLock(innerNgState match {
      case Some(ng) =>
        state.leaseDetails(leaseId).map(ld =>
          ld.copy(isActive = ng.bestLiquidDiff.leaseMap.get(leaseId).map(_.isActive).getOrElse(ld.isActive))) orElse
          ng.bestLiquidDiff.leaseMap.get(leaseId)
      case None =>
        state.leaseDetails(leaseId)
    })

  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee = readLock {
    innerNgState.fold(state.filledVolumeAndFee(orderId))(_.bestLiquidDiff.orderFills.get(orderId).orEmpty.combine(state.filledVolumeAndFee(orderId)))
  }

  override def addressBalanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot] = readLock {
    if (to <= state.height || innerNgState.isEmpty) {
      state.addressBalanceSnapshots(address, from, to)
    } else {
      val bs = BalanceSnapshot(height, addressPortfolio(address))
      if (state.height > 0 && from < this.height) bs +: state.addressBalanceSnapshots(address, from, to) else Seq(bs)
    }
  }

  override def contractBalanceSnapshots(contractId: ContractId, from: Int, to: Int): Seq[BalanceSnapshot] = readLock {
    if (to <= state.height || innerNgState.isEmpty) {
      state.contractBalanceSnapshots(contractId, from, to)
    } else {
      val bs = BalanceSnapshot(height, contractPortfolio(contractId))
      if (state.height > 0 && from < this.height) bs +: state.contractBalanceSnapshots(contractId, from, to) else Seq(bs)
    }
  }

  override def accounts(): Set[Address] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).accounts()
  }

  override def accountScript(address: Address): Option[Script] =
    readLock(innerNgState.fold(state.accountScript(address)) { ng =>
      ng.bestLiquidDiff.scripts.get(address) match {
        case None      => state.accountScript(address)
        case Some(scr) => scr
      }
    })

  override def hasScript(address: Address): Boolean = readLock {
    innerNgState.fold(state.hasScript(address)) { ng =>
      ng.bestLiquidDiff.scripts.exists {
        case (addr, maybeScript) => addr == address && maybeScript.nonEmpty
      } || state.hasScript(address)
    }
  }

  override def assetScript(asset: AssetId): Option[Script] =
    readLock(innerNgState.fold(state.assetScript(asset)) { ng =>
      ng.bestLiquidDiff.assetScripts.get(asset) match {
        case None      => state.assetScript(asset)
        case Some(scr) => scr
      }
    })

  override def hasAssetScript(asset: AssetId): Boolean =
    readLock(innerNgState.fold(state.hasAssetScript(asset)) { ng =>
      ng.bestLiquidDiff.assetScripts.get(asset) match {
        case None    => state.hasAssetScript(asset)
        case Some(x) => x.nonEmpty
      }
    })

  def accountDataSlice(acc: Address, from: Int, to: Int): AccountDataInfo =
    readLock(innerNgState.fold(state.accountData(acc)) { ng =>
      val fromInner = state.accountDataSlice(acc, from, to)
      val fromDiff  = ng.bestLiquidDiff.accountData.get(acc).orEmpty
      fromInner.combine(fromDiff)
    })

  override def accountData(acc: Address): AccountDataInfo =
    readLock(innerNgState.fold(state.accountData(acc)) { ng =>
      val fromInner = state.accountData(acc)
      val fromDiff  = ng.bestLiquidDiff.accountData.get(acc).orEmpty
      fromInner.combine(fromDiff)
    })

  override def accountData(acc: Address, key: String): Option[DataEntry[_]] =
    readLock(innerNgState.fold(state.accountData(acc, key)) { ng =>
      val diffData = ng.bestLiquidDiff.accountData.get(acc).orEmpty
      diffData.data.get(key).orElse(state.accountData(acc, key))
    })

  private def changedBalances(pred: Portfolio => Boolean, f: AssetHolder => Long): Map[AssetHolder, Long] = readLock {
    innerNgState
      .fold(Map.empty[AssetHolder, Long]) { ng =>
        for {
          (owner, p) <- ng.bestLiquidDiff.portfolios
          if pred(p)
        } yield owner -> f(owner)
      }
  }

  override def addressAssetDistribution(assetId: AssetId): AssetDistribution = readLock {
    val fromInner = state.addressAssetDistribution(assetId)
    val fromNg    = AssetDistribution(changedBalances(_.assets.getOrElse(assetId, 0L) != 0, assetHolderBalance(_, Some(assetId))).collectAddresses)

    fromInner |+| fromNg
  }

  override def addressAssetDistributionAtHeight(assetId: AssetId,
                                                height: Int,
                                                count: Int,
                                                fromAddress: Option[Address]): Either[ValidationError, AssetDistributionPage] = readLock {
    state.addressAssetDistributionAtHeight(assetId, height, count, fromAddress)
  }

  override def addressWestDistribution(height: Int): Map[Address, Long] =
    readLock(innerNgState.fold(state.addressWestDistribution(height)) { _ =>
      val innerDistribution = state.addressWestDistribution(height)
      if (height < this.height) innerDistribution
      else {
        innerDistribution ++ changedBalances(_.balance != 0, assetHolderBalance(_)).collectAddresses
      }
    })

  /** Builds a new portfolio map by applying a partial function to all portfolios on which the function is defined.
    *
    * @note Portfolios passed to `pf` only contain WEST and Leasing balances to improve performance */
  override def collectAddressLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A] = readLock {
    innerNgState.fold(state.collectAddressLposPortfolios(pf)) { ng =>
      val b = Map.newBuilder[Address, A]
      for ((a, p) <- ng.bestLiquidDiff.portfolios.collectAddresses if p.lease != LeaseBalance.empty || p.balance != 0) {
        pf.runWith(b += a -> _)(a -> this.addressWestPortfolio(a))
      }

      state.collectAddressLposPortfolios(pf) ++ b.result()
    }
  }

  override def append(
      diff: Diff,
      carry: Long,
      block: Block,
      consensusPostActionDiff: ConsensusPostActionDiff,
      certificates: Set[X509Certificate]
  ): Int = writeLock {
    state.append(diff, carry, block, consensusPostActionDiff, certificates)
  }

  override def rollbackTo(targetBlockId: AssetId): Either[String, RollbackResult] = writeLock {
    state.rollbackTo(targetBlockId)
  }

  override def transactionHeight(id: AssetId): Option[Int] = readLock {
    innerNgState flatMap { ng =>
      ng.bestLiquidDiff.transactionsMap.get(id).map(_._1)
    } orElse state.transactionHeight(id)
  }

  override def addressBalance(address: Address, mayBeAssetId: Option[AssetId]): Long =
    readLock(innerNgState match {
      case Some(ng) =>
        state
          .addressBalance(address, mayBeAssetId) + ng.bestLiquidDiff.portfolios
          .getOrElse(address.toAssetHolder, Portfolio.empty)
          .balanceOf(mayBeAssetId)
      case None =>
        state.addressBalance(address, mayBeAssetId)
    })

  override def contractBalance(contractId: ContractId, mayBeAssetId: Option[AssetId], readingContext: ContractReadingContext): Long =
    readLock(innerNgState match {
      case Some(ng) =>
        state.contractBalance(contractId, mayBeAssetId, readingContext) + ng.bestLiquidDiff.portfolios
          .getOrElse(contractId.toAssetHolder, Portfolio.empty)
          .balanceOf(mayBeAssetId)
      case None => state.contractBalance(contractId, mayBeAssetId, readingContext)
    })

  override def permissions(acc: Address): Permissions = readLock {
    val ngPermissions = innerNgState.fold(Diff.empty)(_.bestLiquidDiff).permissions.getOrElse(acc, Permissions.empty)
    val bcPermissions = state.permissions(acc)
    ngPermissions.combine(bcPermissions)
  }

  override def miners: MinerQueue = readLock {
    val ngMiners = MinerQueue(innerNgState.fold(Diff.empty)(_.bestLiquidDiff).permissions)
    val bcMiners = state.miners
    ngMiners.combine(bcMiners)
  }

  override def contractValidators: ContractValidatorPool = readLock {
    // We do not use NG, because need validators from last hard block
    state.contractValidators
  }

  override def lastBlockContractValidators: Set[Address] = readLock {
    innerNgState
      .filter(_.baseBlockType == Hard)
      .fold(state.lastBlockContractValidators) { ng =>
        val timestamp  = ng.base.timestamp
        val validators = state.contractValidators.update(ng.baseBlockDiff.permissions).currentValidatorSet(timestamp)
        log.trace(s"Contract validator set based on NG hard-block at '$timestamp': '${validators.mkString("', '")}'")
        validators
      }
  }

  override def minerBanHistory(address: Address): MinerBanHistory = readLock {
    (for {
      ng                <- innerNgState
      ngMinerBanHistory <- ng.consensusPostActionDiff.minersBanHistory.get(address)
    } yield {
      if (activatedFeatures.get(BlockchainFeature.MinerBanHistoryOptimisationFix.id).exists(_ <= height)) {
        ngMinerBanHistory
      } else {
        // The history combination is not necessary, because NG history is full.
        // But the modification of the logic will affect the previously generated state with MinerBanHistoryV1.
        (state.minerBanHistory(address), ngMinerBanHistory) match {
          case (stateHistory: MinerBanHistoryV1, ngHistory: MinerBanHistoryV1) => stateHistory combine ngHistory
          case _                                                               => ngMinerBanHistory
        }
      }
    }).getOrElse {
      state.minerBanHistory(address)
    }
  }

  override def bannedMiners(height: Int): Seq[Address] = readLock {
    val innerBannedMiners = state.bannedMiners(height)

    innerNgState.fold(innerBannedMiners) { ng =>
      innerBannedMiners.filter { address =>
        val addressMinersBanHistoryDiff = ng.consensusPostActionDiff.minersBanHistory.get(address)
        addressMinersBanHistoryDiff.forall(_.isBanned(height))
      }
    }
  }

  override def warningsAndBans: Map[Address, Seq[MinerBanlistEntry]] = readLock {
    val innerWarningsAndBans = state.warningsAndBans

    innerNgState.fold(innerWarningsAndBans) { ng =>
      innerWarningsAndBans.map {
        case (address, warningsAndBans) =>
          val addressMinersBanHistoryDiff = ng.consensusPostActionDiff.minersBanHistory.get(address)
          address -> addressMinersBanHistoryDiff.fold(warningsAndBans)(_.entries)
      }
    }
  }

  override def participantPubKey(address: Address): Option[PublicKeyAccount] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).participantPubKey(address)
  }

  def networkParticipants(): Seq[Address] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).networkParticipants()
  }

  override def policies(): Set[ByteStr] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).policies()
  }

  override def policyExists(policyId: ByteStr): Boolean = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).policyExists(policyId)
  }

  override def policyOwners(policyId: ByteStr): Set[Address] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).policyOwners(policyId)
  }

  override def policyRecipients(policyId: ByteStr): Set[Address] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).policyRecipients(policyId)
  }

  override def lastMicroBlock: Option[MicroBlock] = readLock {
    innerNgState.flatMap(_.lastMicroBlock)
  }

  override def currentBaseBlock: Option[Block] = readLock {
    innerNgState.map(_.base)
  }

  override def contracts(): Set[ContractInfo] = readLock {
    val fromInner = state.contracts().map(c => c.contractId -> c).toMap
    val fromDiff  = innerNgState.fold(Diff.empty)(_.bestLiquidDiff).contracts
    (fromInner ++ fromDiff).values.toSet
  }

  override def contract(contractId: ContractId): Option[ContractInfo] = readLock {
    val contractOpt = innerNgState.fold(Diff.empty)(_.bestLiquidDiff).contracts.get(contractId)
    contractOpt.orElse(state.contract(contractId))
  }

  override def contractKeys(keysRequest: KeysRequest, readingContext: ContractReadingContext): Vector[String] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).contractKeys(keysRequest, readingContext)
  }

  override def contractData(contractId: ByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).contractData(contractId, keys, readingContext)
  }

  override def contractData(contractId: ByteStr, readingContext: ContractReadingContext): ExecutedContractData =
    readLock(innerNgState.fold(state.contractData(contractId, readingContext)) { ng =>
      val fromInner = state.contractData(contractId, readingContext)
      val fromDiff  = ng.bestLiquidDiff.contractsData.get(contractId).orEmpty
      fromInner.combine(fromDiff)
    })

  override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] =
    readLock(innerNgState.fold(state.contractData(contractId, key, readingContext)) { ng =>
      val diffData = ng.bestLiquidDiff.contractsData.get(contractId).orEmpty
      diffData.data.get(key).orElse(state.contractData(contractId, key, readingContext))
    })

  override def executedTxFor(forTxId: AssetId): Option[ExecutedContractTransaction] =
    readLock(innerNgState.fold(state.executedTxFor(forTxId)) { ng =>
      val txIdOptFromDiff = ng.bestLiquidDiff.executedTxMapping.get(forTxId)
      txIdOptFromDiff
        .flatMap(transactionInfo)
        .map(_._2)
        .map(_.asInstanceOf[ExecutedContractTransaction])
        .orElse(state.executedTxFor(forTxId))
    })

  override def hasExecutedTxFor(forTxId: ByteStr): Boolean = readLock {
    innerNgState.fold(state.hasExecutedTxFor(forTxId)) { ng =>
      ng.bestLiquidDiff.executedTxMapping.contains(forTxId) || state.hasExecutedTxFor(forTxId)
    }
  }

  override def policyDataHashes(policyId: ByteStr): Set[PolicyDataHash] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).policyDataHashes(policyId)
  }

  override def policyDataHashExists(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = readLock {
    policyDataHashes(policyId).contains(dataHash)
  }

  override def allNonEmptyRoleAddresses: Stream[Address] = readLock {
    CompositeBlockchain.composite(state, innerNgState.map(_.bestLiquidDiff)).allNonEmptyRoleAddresses
  }

  override def pendingPrivacyItems(): Set[PolicyDataId] = readLock {
    state.pendingPrivacyItems()
  }

  override def addToPending(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = writeLock {
    state.addToPending(policyId, dataHash)
  }

  override def addToPending(policyDataIds: Set[PolicyDataId]): Int = writeLock {
    state.addToPending(policyDataIds)
  }

  override def isPending(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = readLock {
    state.isPending(policyId, dataHash)
  }

  override def removeFromPendingAndLost(policyId: ByteStr, dataHash: PolicyDataHash): (Boolean, Boolean) = writeLock {
    state.removeFromPendingAndLost(policyId, dataHash)
  }

  override def pendingToLost(policyId: ByteStr, dataHash: PolicyDataHash): (Boolean, Boolean) = writeLock {
    state.pendingToLost(policyId, dataHash)
  }

  override def lostPrivacyItems(): Set[PolicyDataId] = readLock {
    state.lostPrivacyItems()
  }

  override def isLost(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = readLock {
    state.isLost(policyId, dataHash)
  }

  override def policyDataHashTxId(id: PolicyDataId): Option[ByteStr] = readLock {
    state.policyDataHashTxId(id)
  }

  override def forceUpdateIfNotInProgress(dataId: PolicyDataId, ownerAddress: Address): Unit =
    writeLock {
      import dataId.{dataHash, policyId}

      if (
        policyRecipients(policyId).contains(ownerAddress) &&
        policyDataHashExists(policyId, dataHash) &&
        !isPending(policyId, dataHash) &&
        !isLost(policyId, dataHash)
      ) {
        log.debug(s"Policy '$policyId' data hash '$dataHash' force sync, because the item was not found in the processing queues")
        if (state.addToPending(policyId, dataHash)) PrivacyMetrics.pendingSizeStats.increment()
        internalPolicyUpdates.onNext(PolicyUpdate(dataId, None))
      }
    }

  override def currentMiner: Option[Address] = innerCurrentMiner

  override def privacyItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash): Option[PrivacyItemDescriptor] = readLock {
    state.privacyItemDescriptor(policyId, dataHash)
  }

  override def putItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash, descriptor: PrivacyItemDescriptor): Unit = writeLock {
    state.putItemDescriptor(policyId, dataHash, descriptor)
  }

  override def certByDistinguishedNameHash(dnHash: String): Option[Certificate] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .certByDistinguishedNameHash(dnHash)
  }

  override def certByDistinguishedName(distinguishedName: String): Option[Certificate] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .certByDistinguishedName(distinguishedName)
  }

  override def certByPublicKey(publicKey: PublicKeyAccount): Option[Certificate] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .certByPublicKey(publicKey)
  }

  override def certByFingerPrint(fingerprint: ByteStr): Option[Certificate] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .certByFingerPrint(fingerprint)
  }

  override def certsAtHeight(height: Int): Set[Certificate] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .certsAtHeight(height)
  }

  override def certChainStoreByBlockId(blockId: BlockId): Option[CertChainStore] = readLock {
    for {
      ng  <- innerNgState
      ccs <- ng.certStoreForBlock(blockId)
    } yield ccs
  }

  override def aliasesIssuedByAddress(address: Address): Set[Alias] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .aliasesIssuedByAddress(address)
  }

  override def crlDataByHash(crlHash: ByteStr): Option[CrlData] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .crlDataByHash(crlHash)
  }

  override def actualCrls(issuer: PublicKeyAccount, timestamp: Long): Set[CrlData] = readLock {
    CompositeBlockchain
      .composite(state, innerNgState.map(_.bestLiquidDiff))
      .actualCrls(issuer, timestamp)
  }

  override def crlHashesByBlockId(blockId: BlockId): Set[ByteStr] = readLock {
    innerNgState.fold(Set.empty[ByteStr])(_.crlHashesForBlock(blockId))
  }
}

object BlockchainUpdaterImpl extends ScorexLogging {

  private val blockMicroForkStats       = Kamon.counter("block-micro-fork")
  private val microMicroForkStats       = Kamon.counter("micro-micro-fork")
  private val microBlockForkStats       = Kamon.counter("micro-block-fork")
  private val microBlockForkHeightStats = Kamon.histogram("micro-block-fork-height")
  private val forgeBlockTimeStats       = Kamon.histogram("forge-block-time", MeasurementUnit.time.milliseconds)

  private def sameConsensuses(b1: Block, b2: Block): Boolean = (b1.consensusData, b2.consensusData) match {
    case (PoSLikeConsensusBlockData(bt1, _), PoSLikeConsensusBlockData(bt2, _)) => bt1 == bt2
    case (PoALikeConsensusBlockData(sr1), PoALikeConsensusBlockData(sr2))       => sr1 == sr2
    case (CftLikeConsensusBlockData(v1, s1), CftLikeConsensusBlockData(v2, s2)) => v1 == v2 && s1 == s2
    case _                                                                      => false
  }

  def areVersionsOfSameBlock(b1: Block, b2: Block): Boolean =
    b1.signerData.generator == b2.signerData.generator &&
      sameConsensuses(b1, b2) &&
      b1.reference == b2.reference &&
      b1.timestamp == b2.timestamp
}
