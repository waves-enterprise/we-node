package com.wavesenterprise.network

import cats.Eq
import com.wavesenterprise.block.MicroBlock
import com.wavesenterprise.metrics.BlockStats
import com.wavesenterprise.network.MicroBlockLoader.ReceivedMicroBlock
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.settings.SynchronizationSettings.MicroblockSynchronizerSettings
import com.wavesenterprise.state.{ByteStr, NG, SignatureValidator}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel._
import monix.eval.Task
import monix.execution.{Cancelable, Scheduler}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import scala.util.Random

/**
  * Loads micro-blocks from random owners when receiving new micro-block inventory.
  */
class MicroBlockLoader(
    ng: NG,
    settings: MicroblockSynchronizerSettings,
    activePeerConnections: ActivePeerConnections,
    incomingMicroBlockInventoryEvents: ChannelObservable[MicroBlockInventory],
    incomingMicroBlockEvents: ChannelObservable[MicroBlockResponse],
    signatureValidator: SignatureValidator,
    val storage: MicroBlockLoaderStorage
)(implicit scheduler: Scheduler)
    extends ScorexLogging
    with AutoCloseable {

  private val internalLoadingUpdates                 = ConcurrentSubject.publish[ReceivedMicroBlock]
  def loadingUpdates: Observable[ReceivedMicroBlock] = internalLoadingUpdates

  private val loading: Cancelable =
    incomingMicroBlockInventoryEvents
      .asyncBoundary(OverflowStrategy.BackPressure(settings.crawlingParallelism.value * 2))
      .mapEval((processInventory _).tupled)
      .collect {
        case Some(newInventory: MicroBlockInventoryV2) if ng.currentBaseBlock.exists(_.signerData.signature == newInventory.keyBlockSig) =>
          newInventory
        case Some(newInventory: MicroBlockInventoryV1) =>
          newInventory
      }
      .mapParallelUnordered(settings.crawlingParallelism.value) { inventory =>
        downloadMicroBlock(inventory, settings.maxDownloadAttempts.value)
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

  private def processInventory(channel: Channel, inventory: MicroBlockInventory): Task[Option[MicroBlockInventory]] = {
    signatureValidator
      .validate(inventory)
      .asyncBoundary(scheduler)
      .map {
        case Left(error) =>
          closeChannel(channel, error.toString)
          None
        case Right(_) =>
          storage.addOwner(channel, inventory.totalBlockSig)
          if (storage.isUnknownInventory(inventory, channel)) {
            log.trace(s"Received new micro-block inventory $inventory from channel '${id(channel)}'")
            Some(inventory)
          } else {
            log.trace(s"Discard micro-block inventory $inventory from channel '${id(channel)}', because storage contains a similar inventory")
            None
          }
      }
  }

  private def downloadMicroBlock(inventory: MicroBlockInventory, maxAttempts: Int, excludeChannels: Set[Channel] = Set.empty): Task[Unit] =
    if (maxAttempts <= 0) {
      Task(log.warn(s"Too many micro-block attempts with signature '${inventory.totalBlockSig}'"))
    } else {
      def retry(nextExcludedChannels: Set[Channel]): Task[Unit] =
        Task.defer(downloadMicroBlock(inventory, maxAttempts - 1, nextExcludedChannels))

      def ifEmptyOwners: Task[Unit] =
        Task(log.warn(s"The owner of the micro-block '${inventory.totalBlockSig}' not found, retrying")) *>
          retry(Set.empty)

      randomOwner(inventory, excludeChannels)
        .fold(ifEmptyOwners) { channel =>
          if (channel.isOpen) {
            requestAndAwait(channel, inventory, maxAttempts, excludeChannels)
          } else {
            Task(log.debug(s"Channel '${id(channel)}' was closed, retrying with other owner")) *>
              retry(excludeChannels + channel)
          }
        }
    }

  private def randomOwner(inventory: MicroBlockInventory, exclude: Set[Channel]): Option[Channel] = {
    val set = storage.currentOwners(inventory.totalBlockSig) -- exclude

    if (set.isEmpty) {
      None
    } else {
      val n = Random.nextInt(set.size)
      Some(set.iterator.drop(n).next())
    }
  }

  private def requestAndAwait(channel: Channel, inventory: MicroBlockInventory, maxAttempts: Int, excludeChannels: Set[Channel]): Task[Unit] = {

    def retry: Task[Unit] = Task.defer {
      log.warn(s"Timed out waiting for channel '${id(channel)}' response with micro-block '${inventory.totalBlockSig}'")
      downloadMicroBlock(inventory, maxAttempts - 1, excludeChannels + channel)
    }

    Task(log.debug(s"Attempting to load micro-block '${inventory.totalBlockSig}' from channel '${id(channel)}'")) *>
      sendRequestAndAwait(channel, incomingMicroBlockEvents)(Task.pure(MicroBlockRequest(inventory.totalBlockSig))) {
        _.collect {
          case (channel, response: MicroBlockResponse) if response.microblock.totalLiquidBlockSig == inventory.totalBlockSig =>
            channel -> ReceivedMicroBlock(channel, inventory, response.microblock, response.certChainStore, response.crlHashes)
        }.mapEval {
          case (channel, entry) =>
            for {
              _ <- prepareEntry(channel, entry)
              _ <- Task {
                storage.put(entry)
                activePeerConnections.broadcast(entry.inventory, except = storage.currentOwners(entry.microBlock.totalLiquidBlockSig))
                BlockStats.received(entry.microBlock, channel)
              }
              _ <- Task.deferFuture(internalLoadingUpdates.onNext(entry)).void
            } yield ()
        }
          .firstL
          .timeoutTo(settings.waitResponseTimeout, retry)
      }
  }

  protected def prepareEntry(channel: Channel, receivedMicroBlock: ReceivedMicroBlock): Task[Unit] = Task.unit

  override def close(): Unit = {
    internalLoadingUpdates.onComplete()
    loading.cancel()
  }
}

object MicroBlockLoader {

  type MicroBlockSignature = ByteStr

  case class ReceivedMicroBlock(channel: Channel,
                                inventory: MicroBlockInventory,
                                microBlock: MicroBlock,
                                certChainStore: CertChainStore,
                                crlHashes: Set[ByteStr])

  object ReceivedMicroBlock {
    implicit val eq: Eq[ReceivedMicroBlock] = { (x, y) =>
      x.microBlock.totalLiquidBlockSig == y.microBlock.totalLiquidBlockSig &&
      x.microBlock.prevLiquidBlockSig == y.microBlock.prevLiquidBlockSig
    }
  }
}
