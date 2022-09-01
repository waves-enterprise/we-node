package com.wavesenterprise.network

import cats.Eq
import cats.syntax.functor._
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.network.BlockLoader.{ExtensionBlocks, State}
import com.wavesenterprise.network.BlockLoader.LoaderState.{Active, ExpectingBlocks, ExpectingSignatures, Idle}
import com.wavesenterprise.network.SyncChannelSelector.{ChannelInfo, SyncChannelUpdateEvent}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel._
import monix.catnap.Semaphore
import monix.eval.{Coeval, Task}
import monix.execution.schedulers.SchedulerService
import monix.execution.{AsyncVar, CancelableFuture}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import scala.concurrent.duration._

/**
  * Observes incoming messages and produces broadcast blocks and extensions events.
  *
  * Extensions - batches of blocks that node requests from current sync channel if it has newer blocks.
  * Broadcast blocks – unrequested blocks that miners broadcast to all their peers.
  *
  * Extension and broadcast blocks observables internally represent bounded queues with back pressure.
  */
class BlockLoader(
    syncTimeOut: FiniteDuration,
    extensionBatchSize: Int,
    lastBlockIdsReporter: Coeval[Seq[ByteStr]],
    invalidBlocks: InvalidBlockStorage,
    incomingBlockEvents: ChannelObservable[BlockWrapper],
    incomingSignatureEvents: ChannelObservable[Signatures],
    incomingMissingBlockEvents: ChannelObservable[MissingBlock],
    channelCloseEvents: Observable[Channel],
    syncChannelUpdateEvents: Observable[SyncChannelUpdateEvent]
)(implicit scheduler: SchedulerService)
    extends AutoCloseable
    with ScorexLogging {

  private val stateAsyncLock               = Semaphore.unsafe[Task](1)
  @volatile private var loaderState: State = State(Idle)

  private val broadcastBlocksQueue = ConcurrentSubject.publish[(Channel, BlockWrapper)](OverflowStrategy.DropOld(10))

  private val extensionsQueue = AsyncVar.empty[(Channel, ExtensionBlocks)]()

  def broadcastBlockEvents: ChannelObservable[BlockWrapper] = {
    val eventEqByBlockId: Eq[(Channel, BlockWrapper)] = {
      case ((_, bw1), (_, bw2)) => bw1.block.uniqueId == bw2.block.uniqueId
    }

    broadcastBlocksQueue.distinctUntilChanged(eventEqByBlockId)
  }

  def extensionEvents: ChannelObservable[ExtensionBlocks] = Observable.repeatEvalF(Task.deferFuture(extensionsQueue.take()))

  def forceUpdate(): Task[Unit] = stateAsyncLock.withPermit {
    Task {
      log.debug(s"Force update")
      loaderState = requestSignaturesIfNeed(loaderState)
    }.void
  }

  private val lastSyncChannel: Coeval[Option[ChannelInfo]] = {
    val channelUpdates = syncChannelUpdateEvents.map(_.currentChannelOpt)
    lastObserved(channelUpdates).map(_.flatten)
  }

  private def onSyncChannelUpdate(state: State, maybeNewChannel: Option[ChannelInfo]): Task[State] =
    Task {
      maybeNewChannel match {
        case Some(bestChannel) =>
          requestSignaturesIfNeed(state, Some(bestChannel))
        case None =>
          state.loaderState match {
            case _: Active => state.withIdleLoader
            case _         => state
          }
      }
    }

  private def onChannelClosed(state: State, channel: Channel) = Task.eval {
    state.loaderState match {
      case active: Active if active.channel == channel => state.withIdleLoader
      case _                                           => state
    }
  }

  private def requestSignaturesIfNeed(state: State,
                                      channel: Option[ChannelInfo] = lastSyncChannel(),
                                      knownSignatures: Seq[BlockId] = lastBlockIdsReporter(),
                                      optimistically: Boolean = false): State = {
    channel match {
      case None =>
        log.debug("Sync channel is None, synchronization not possible")
        state.withIdleLoader
      case Some(best) =>
        state match {
          case State(Idle) =>
            requestSignatures(state, best.channel, knownSignatures, optimistically)
          case _ =>
            log.debug(s"No need to request new signatures, state '$state'")
            state
        }
    }
  }

  private def scheduleTimeoutHandling(channel: Channel, reason: String): Task[Unit] =
    Task
      .defer {
        closeChannel(channel, reason)
        extensionsQueue.tryTake()
        stateAsyncLock.withPermit(Task {
          loaderState = State(Idle)
        })
      }
      .delayExecution(syncTimeOut)

  private def requestSignatures(state: State, channel: Channel, knownSignatures: Seq[BlockId], optimistically: Boolean): State = {
    val optimisticallyStatus = if (optimistically) " optimistically " else " "
    log.debug(
      s"Requesting signatures${optimisticallyStatus}from '${id(channel)}', last '${knownSignatures.length}' " +
        s"known are '${formatSignatures(knownSignatures)}'")
    val timeoutHandler = scheduleTimeoutHandling(channel, s"Timeout loading extension signatures").runToFuture
    channel.writeAndFlush(GetNewSignatures(knownSignatures))
    state.withLoaderState(ExpectingSignatures(channel, knownSignatures.toSet, timeoutHandler, optimistically))
  }

  private def onSignatures(state: State, channel: Channel, signatures: Seq[ByteStr]): Task[State] = Task.eval {
    state.loaderState match {
      case ExpectingSignatures(ch, known, _, _) if ch == channel =>
        val (_, unknown) = signatures.span(known.contains)

        val maybeFirstInvalid = signatures.view.flatMap { sig =>
          invalidBlocks.find(sig).map(sig -> _)
        }.headOption

        maybeFirstInvalid match {
          case Some((invalidBlock, reason)) =>
            closeChannel(channel, s"Signatures contains previously processed invalid block '$invalidBlock', invalidation reason '$reason'")
            state.withIdleLoader
          case None =>
            if (unknown.isEmpty) {
              log.debug(s"Received signatures from '${id(channel)}' do not contain new")
              state.withIdleLoader
            } else {
              log.debug(s"Received signatures '${formatSignatures(signatures)}' from '${id(channel)}' contains new '${formatSignatures(unknown)}'")
              requestBlockBatch(channel, state, unknown)
            }
        }
      case otherState =>
        log.debug(
          s"Received unexpected signatures '${formatSignatures(signatures)}' from '${id(channel)}', ignoring it. Current state '$otherState'.")
        state
    }
  }

  private def onBlock(state: State, channel: Channel, blockWrapper: BlockWrapper): Task[State] = Task.defer {
    val blockId = blockWrapper.block.uniqueId
    state.loaderState match {
      case ExpectingBlocks(ch, requested, expected, received, rest, timeoutHandler) if ch == channel && expected.contains(blockId) =>
        val isLastExpected = Set(blockId) == expected

        if (isLastExpected) {
          timeoutHandler.cancel()

          val blockById              = (received + blockWrapper).map(bw => bw.block.uniqueId -> bw).toMap
          val requestedOrderedBlocks = requested.map(blockById)
          val extensionBlocks        = ExtensionBlocks(requestedOrderedBlocks)
          log.debug(s"Expected extension blocks '$extensionBlocks' successfully received from '${id(channel)}'")
          val extensionSignatures = requestedOrderedBlocks.map(_.block.uniqueId)

          Task.fromFuture(extensionsQueue.put(ch -> extensionBlocks)) >>
            (if (rest.nonEmpty) {
               Task(requestBlockBatch(channel, state, rest))
             } else {
               Task(requestSignaturesIfNeed(state.withIdleLoader, knownSignatures = extensionSignatures.reverse, optimistically = true))
             })
        } else {
          val timeoutHandler =
            scheduleTimeoutHandling(channel, s"Timeout loading one of requested blocks, non-received: ${expected.size - 1}").runToFuture
          Task(state.withLoaderState(ExpectingBlocks(ch, requested, expected - blockId, received + blockWrapper, rest, timeoutHandler)))
        }
      case _: ExpectingBlocks =>
        log.debug(s"Received unexpected block '$blockId' from '${id(channel)}', ignoring it")
        Task.pure(state)
      case _ =>
        log.trace(s"Received broadcast block '$blockId' from '${id(channel)}'")
        Task.fromFuture(broadcastBlocksQueue.onNext(channel -> blockWrapper)).as(state)
    }
  }

  private def onMissingBlock(state: State, channel: Channel, missingBlock: MissingBlock): Task[State] = Task.defer {
    state.loaderState match {
      case ExpectingBlocks(ch, _, expected, _, _, _) if ch == channel && expected.contains(missingBlock.signature) =>
        Task {
          log.debug(s"Requested block '${missingBlock.signature.base58}' is missing. Requesting new signatures")
          requestSignaturesIfNeed(state.withIdleLoader)
        }
      case _ =>
        log.debug(s"Received unexpected missing block '${missingBlock.signature.base58}' from '${id(channel)}', ignoring it")
        Task.pure(state)
    }
  }

  private def requestBlockBatch(channel: Channel, state: State, allSignatures: Seq[BlockId]): State = {
    val (batch, rest) = allSignatures.splitAt(extensionBatchSize)
    log.debug(s"Requesting next blocks batch '${formatSignatures(batch)}'")
    val timeoutHandler = scheduleTimeoutHandling(channel, "Timeout loading first requested batch block").runToFuture
    channel.writeAndFlush(GetBlocks(batch))

    state.withLoaderState(ExpectingBlocks(channel, batch, batch.toSet, Set.empty, rest, timeoutHandler))
  }

  val stateReporter: Coeval[State] = Coeval.eval(loaderState)

  private sealed trait StateUpdateReason
  private case class SyncChannelUpdate(newChannelOpt: Option[ChannelInfo])              extends StateUpdateReason
  private case class SignaturesReceived(channel: Channel, signatures: Signatures)       extends StateUpdateReason
  private case class BlockReceived(channel: Channel, block: BlockWrapper)               extends StateUpdateReason
  private case class ChannelClosed(channel: Channel)                                    extends StateUpdateReason
  private case class MissingBlockReceived(channel: Channel, missingBlock: MissingBlock) extends StateUpdateReason

  private def updateState(reason: StateUpdateReason): Task[Unit] = Task.defer {
    val calcNewState = reason match {
      case SyncChannelUpdate(maybeNewChannel)     => onSyncChannelUpdate(loaderState, maybeNewChannel)
      case ChannelClosed(ch)                      => onChannelClosed(loaderState, ch)
      case SignaturesReceived(ch, event)          => onSignatures(loaderState, ch, event.signatures)
      case BlockReceived(ch, block)               => onBlock(loaderState, ch, block)
      case MissingBlockReceived(ch, missingBlock) => onMissingBlock(loaderState, ch, missingBlock)
    }

    calcNewState.map(loaderState = _)
  }

  private val loading: CancelableFuture[Unit] =
    Observable(
      syncChannelUpdateEvents.map(event => SyncChannelUpdate(event.currentChannelOpt)),
      channelCloseEvents.map(ChannelClosed),
      incomingSignatureEvents.map((SignaturesReceived.apply _).tupled),
      incomingBlockEvents.map((BlockReceived.apply _).tupled),
      incomingMissingBlockEvents.map((MissingBlockReceived.apply _).tupled)
    ).merge
      .asyncBoundary(OverflowStrategy.Default)
      .doOnNext(reason => Task.eval(log.trace(s"State update because '$reason'")))
      .mapEval(reason => stateAsyncLock.withPermit(updateState(reason)))
      .logErr
      .foreach(_ => log.trace(s"New state '$loaderState'"))

  override def close(): Unit = {
    loading.cancel()
  }
}

object BlockLoader extends ScorexLogging {

  sealed trait LoaderState

  object LoaderState {

    sealed trait Active extends LoaderState {
      def channel: Channel
      def timeoutHandler: CancelableFuture[Unit]
    }

    case object Idle extends LoaderState

    case class ExpectingSignatures(channel: Channel, known: Set[BlockId], timeoutHandler: CancelableFuture[Unit], optimistically: Boolean)
        extends Active {
      override def toString: String =
        s"ExpectingSignatures(channel='${id(channel)}',known=${known.size}${if (optimistically) ", optimistically" else ""})"
    }

    case class ExpectingBlocks(channel: Channel,
                               requested: Seq[BlockId],
                               expected: Set[BlockId],
                               received: Set[BlockWrapper],
                               rest: Seq[BlockId],
                               timeoutHandler: CancelableFuture[Unit])
        extends Active {
      override def toString: String =
        s"ExpectingBlocks(channel=${id(channel)}, totalBlocks=${requested.size}, " +
          s"received=${received.size}, expected=${if (expected.size == 1) expected.head.trim else expected.size})"
    }
  }

  case class ExtensionBlocks(blocks: Seq[BlockWrapper]) {
    override def toString: String = s"ExtensionBlocks(${formatSignatures(blocks.map(_.block.uniqueId))}"
  }

  case class State(loaderState: LoaderState) {
    def withLoaderState(newLoaderState: LoaderState): State = {
      loaderState match {
        case active: Active => active.timeoutHandler.cancel()
        case _              => ()
      }
      State(newLoaderState)
    }

    def withIdleLoader: State = withLoaderState(Idle)
  }
}
