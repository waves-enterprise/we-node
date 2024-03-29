package com.wavesenterprise.network

import com.wavesenterprise.consensus.Vote
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import monix.eval.Coeval
import monix.execution.Scheduler
import monix.reactive.OverflowStrategy
import monix.reactive.subjects.ConcurrentSubject

@Sharable
class MessageObserver(txBufferSize: Int, implicit val scheduler: Scheduler) extends ChannelInboundHandlerAdapter with ScorexLogging {

  private val signatures                    = ConcurrentSubject.publish[(Channel, Signatures)]
  private val blocks                        = ConcurrentSubject.publish[(Channel, BlockWrapper)]
  private val blockchainScores              = ConcurrentSubject.publish[(Channel, BigInt)]
  private val microblockInventories         = ConcurrentSubject.publish[(Channel, MicroBlockInventory)]
  private val microblockResponses           = ConcurrentSubject.publish[(Channel, MicroBlockResponse)]
  private val transactions                  = ConcurrentSubject.publish[(Channel, TxWithSize)](OverflowStrategy.DropNewAndSignal(txBufferSize, onOverflow))
  private val contractsExecutions           = ConcurrentSubject.publish[(Channel, NetworkContractExecutionMessage)]
  private val privateDataRequests           = ConcurrentSubject.publish[(Channel, PrivateDataRequest)]
  private val privateDataResponses          = ConcurrentSubject.publish[(Channel, PrivateDataResponse)]
  private val validatorResults              = ConcurrentSubject.publish[(Channel, ContractValidatorResults)]
  private val blockVotes                    = ConcurrentSubject.publish[(Channel, Vote)]
  private val snapshotNotifications         = ConcurrentSubject.publish[(Channel, SnapshotNotification)]
  private val snapshotRequests              = ConcurrentSubject.publish[(Channel, SnapshotRequest)]
  private val genesisSnapshotRequests       = ConcurrentSubject.publish[(Channel, GenesisSnapshotRequest)]
  private val genesisSnapshotErrors         = ConcurrentSubject.publish[(Channel, GenesisSnapshotError)]
  private val privacyInventories            = ConcurrentSubject.publish[(Channel, PrivacyInventory)]
  private val privacyInventoryRequests      = ConcurrentSubject.publish[(Channel, PrivacyInventoryRequest)]
  private val missingBlocks                 = ConcurrentSubject.publish[(Channel, MissingBlock)]
  private val nodeAttributes                = ConcurrentSubject.publish[(Channel, RawAttributes)]
  private val missingCrlByIdRequests        = ConcurrentSubject.publish[(Channel, MissingCrlDataRequest)]
  private val missingCrlByHashes            = ConcurrentSubject.publish[(Channel, CrlDataByHashesRequest)]
  private val missingCrlByTimestampRange    = ConcurrentSubject.publish[(Channel, CrlDataByTimestampRangeRequest)]
  private val crlDataResponses              = ConcurrentSubject.publish[(Channel, CrlDataResponse)]
  private val confidentialDataRequests      = ConcurrentSubject.publish[(Channel, ConfidentialDataRequest)]
  private val confidentialDataResponses     = ConcurrentSubject.publish[(Channel, ConfidentialDataResponse)]
  private val confidentialInventories       = ConcurrentSubject.publish[(Channel, ConfidentialInventory)]
  private val confidentialInventoryRequests = ConcurrentSubject.publish[(Channel, ConfidentialInventoryRequest)]

  private def onOverflow(numberOfDroppedTx: Long): Coeval[Option[Nothing]] = {
    log.warn(s"Incoming transaction buffer overflow. Number of dropped transaction $numberOfDroppedTx")
    Coeval(None)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case b: BlockWrapper                      => blocks.onNext((ctx.channel(), b))
    case sc: BigInt                           => blockchainScores.onNext((ctx.channel(), sc))
    case s: Signatures                        => signatures.onNext((ctx.channel(), s))
    case mbInv: MicroBlockInventory           => microblockInventories.onNext((ctx.channel(), mbInv))
    case mb: MicroBlockResponse               => microblockResponses.onNext((ctx.channel(), mb))
    case tx: TxWithSize                       => transactions.onNext((ctx.channel(), tx))
    case cem: NetworkContractExecutionMessage => contractsExecutions.onNext((ctx.channel(), cem))
    case pdr: PrivateDataRequest              => privateDataRequests.onNext((ctx.channel(), pdr))
    case pdr: PrivateDataResponse             => privateDataResponses.onNext((ctx.channel(), pdr))
    case cvr: ContractValidatorResults        => validatorResults.onNext((ctx.channel(), cvr))
    case voteMessage: VoteMessage             => blockVotes.onNext((ctx.channel(), voteMessage.vote))
    case n: SnapshotNotification              => snapshotNotifications.onNext((ctx.channel(), n))
    case r: SnapshotRequest                   => snapshotRequests.onNext((ctx.channel(), r))
    case r: GenesisSnapshotRequest            => genesisSnapshotRequests.onNext((ctx.channel(), r))
    case e: GenesisSnapshotError              => genesisSnapshotErrors.onNext((ctx.channel(), e))
    case i: PrivacyInventory                  => privacyInventories.onNext((ctx.channel(), i))
    case r: PrivacyInventoryRequest           => privacyInventoryRequests.onNext((ctx.channel(), r))
    case m: MissingBlock                      => missingBlocks.onNext((ctx.channel(), m))
    case a: RawAttributes                     => nodeAttributes.onNext((ctx.channel(), a))
    case r: MissingCrlDataRequest             => missingCrlByIdRequests.onNext((ctx.channel(), r))
    case r: CrlDataByHashesRequest            => missingCrlByHashes.onNext((ctx.channel(), r))
    case r: CrlDataByTimestampRangeRequest    => missingCrlByTimestampRange.onNext((ctx.channel(), r))
    case c: CrlDataResponse                   => crlDataResponses.onNext((ctx.channel(), c))
    case r: ConfidentialDataRequest           => confidentialDataRequests.onNext((ctx.channel(), r))
    case d: ConfidentialDataResponse          => confidentialDataResponses.onNext((ctx.channel(), d))
    case r: ConfidentialInventoryRequest      => confidentialInventoryRequests.onNext((ctx.channel(), r))
    case i: ConfidentialInventory             => confidentialInventories.onNext((ctx.channel(), i))
    case _                                    => super.channelRead(ctx, msg)
  }

  def shutdown(): Unit = {
    signatures.onComplete()
    blocks.onComplete()
    blockchainScores.onComplete()
    microblockInventories.onComplete()
    microblockResponses.onComplete()
    transactions.onComplete()
    contractsExecutions.onComplete()
    privateDataRequests.onComplete()
    privateDataResponses.onComplete()
    validatorResults.onComplete()
    blockVotes.onComplete()
    snapshotNotifications.onComplete()
    snapshotRequests.onComplete()
    genesisSnapshotRequests.onComplete()
    genesisSnapshotErrors.onComplete()
    privacyInventories.onComplete()
    privacyInventoryRequests.onComplete()
    missingBlocks.onComplete()
    nodeAttributes.onComplete()
    missingCrlByIdRequests.onComplete()
    missingCrlByHashes.onComplete()
    missingCrlByTimestampRange.onComplete()
    crlDataResponses.onComplete()
    confidentialDataRequests.onComplete()
    confidentialDataResponses.onComplete()
    confidentialInventoryRequests.onComplete()
    confidentialInventories.onComplete()
  }
}

object MessageObserver {
  class IncomingMessages(
      val signatures: ChannelObservable[Signatures],
      val blocks: ChannelObservable[BlockWrapper],
      val blockchainScores: ChannelObservable[BigInt],
      val microblockInvs: ChannelObservable[MicroBlockInventory],
      val microblockResponses: ChannelObservable[MicroBlockResponse],
      val transactions: ChannelObservable[TxWithSize],
      val contractsExecutions: ChannelObservable[NetworkContractExecutionMessage],
      val privateDataRequests: ChannelObservable[PrivateDataRequest],
      val privateDataResponses: ChannelObservable[PrivateDataResponse],
      val contractValidatorResults: ChannelObservable[ContractValidatorResults],
      val blockVotes: ChannelObservable[Vote],
      val snapshotNotifications: ChannelObservable[SnapshotNotification],
      val snapshotRequests: ChannelObservable[SnapshotRequest],
      val genesisSnapshotRequests: ChannelObservable[GenesisSnapshotRequest],
      val genesisSnapshotErrors: ChannelObservable[GenesisSnapshotError],
      val privacyInventories: ChannelObservable[PrivacyInventory],
      val privacyInventoryRequests: ChannelObservable[PrivacyInventoryRequest],
      val missingBlocks: ChannelObservable[MissingBlock],
      val nodeAttributes: ChannelObservable[RawAttributes],
      val missingCrlByIdRequests: ChannelObservable[MissingCrlDataRequest],
      val missingCrlByHashes: ChannelObservable[CrlDataByHashesRequest],
      val missingCrlByTimestampRange: ChannelObservable[CrlDataByTimestampRangeRequest],
      val crlDataResponses: ChannelObservable[CrlDataResponse],
      val confidentialDataRequests: ChannelObservable[ConfidentialDataRequest],
      val confidentialDataResponses: ChannelObservable[ConfidentialDataResponse],
      val confidentialInventoryRequests: ChannelObservable[ConfidentialInventoryRequest],
      val confidentialInventories: ChannelObservable[ConfidentialInventory]
  )

  def apply(txBufferSize: Int, scheduler: Scheduler): (MessageObserver, IncomingMessages) = {
    val mo = new MessageObserver(txBufferSize, scheduler)
    val messages = new IncomingMessages(
      mo.signatures,
      mo.blocks,
      mo.blockchainScores,
      mo.microblockInventories,
      mo.microblockResponses,
      mo.transactions,
      mo.contractsExecutions,
      mo.privateDataRequests,
      mo.privateDataResponses,
      mo.validatorResults,
      mo.blockVotes,
      mo.snapshotNotifications,
      mo.snapshotRequests,
      mo.genesisSnapshotRequests,
      mo.genesisSnapshotErrors,
      mo.privacyInventories,
      mo.privacyInventoryRequests,
      mo.missingBlocks,
      mo.nodeAttributes,
      mo.missingCrlByIdRequests,
      mo.missingCrlByHashes,
      mo.missingCrlByTimestampRange,
      mo.crlDataResponses,
      mo.confidentialDataRequests,
      mo.confidentialDataResponses,
      mo.confidentialInventoryRequests,
      mo.confidentialInventories
    )
    (mo, messages)
  }
}
