package com.wavesenterprise.network.privacy

import cats.data.EitherT
import cats.implicits._
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.google.common.primitives.Ints
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.EntityAlreadyExists
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.{CryptoError, EncryptedForSingle, StreamCipher}
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.metrics.privacy.PrivacyMeasurementType._
import com.wavesenterprise.metrics.privacy.PrivacyMetrics
import com.wavesenterprise.network.Attributes.TlsAttribute
import com.wavesenterprise.network.NetworkServer.MetaMessageCodecHandlerName
import com.wavesenterprise.network._
import com.wavesenterprise.network.netty.handler.stream.StreamReadProgressListener
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnection, PeerSession}
import com.wavesenterprise.network.privacy.PolicyDataStreamEncoding.PolicyDataStreamResponse
import com.wavesenterprise.network.privacy.PolicyDataSynchronizerError._
import com.wavesenterprise.privacy._
import com.wavesenterprise.settings.privacy.PrivacySynchronizerSettings
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.{BlockchainUpdater, PolicyUpdate}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import io.netty.channel.{Channel, ChannelId}
import monix.catnap.{MVar, Semaphore}
import monix.eval.{Fiber, Task}
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import monix.execution.exceptions.UpstreamTimeoutException
import monix.reactive.{Consumer, Observable, OverflowStrategy}

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

sealed abstract class PolicyDataSynchronizerError(val message: String, val maybeCause: Option[Throwable] = None)
    extends RuntimeException(message, maybeCause.orNull)

object PolicyDataSynchronizerError {
  case class NoPeerInfo(ch: Channel) extends PolicyDataSynchronizerError(s"No peer info for channel '${id(ch)}'")

  case class MalformedStreamError(policyId: ByteStr, dataHash: PolicyDataHash)
      extends PolicyDataSynchronizerError(s"Malformed response stream for policy '$policyId' data '$dataHash'")

  case class DataNotFoundError(policyId: ByteStr, dataHash: PolicyDataHash)
      extends PolicyDataSynchronizerError(s"Data for policy '$policyId' with dataHash '$dataHash' not found")

  case class PeerOverloadedError(channel: Channel, policyId: ByteStr, dataHash: PolicyDataHash)
      extends PolicyDataSynchronizerError(s"Peer '${id(channel)}' overload. Failed to load policy '$policyId' data '$dataHash'")

  case class LostDataException(policyId: ByteStr, dataHash: PolicyDataHash)
      extends PolicyDataSynchronizerError(s"Data for policy '$policyId' with dataHash '$dataHash' marked as lost")

  case class DecryptionError(policyId: ByteStr, dataHash: PolicyDataHash, cause: CryptoError)
      extends PolicyDataSynchronizerError(s"Failed to decrypt policy '$policyId' data '$dataHash': ${cause.message}")

  case class DeserializingError(bytes: ByteStr, cause: Throwable)
      extends PolicyDataSynchronizerError(s"Error while deserializing bytes: '$bytes'", Some(cause))

  case class StorageUnavailable(storageError: ApiError) extends PolicyDataSynchronizerError(s"Storage unavailable: $storageError")
}

trait PolicyDataSynchronizer {
  def run(): Unit
  def forceSync(): Int
  def forceSync(policyId: ByteStr): Int
  def close(): Unit
}

object NoOpPolicyDataSynchronizer extends PolicyDataSynchronizer {
  override def run(): Unit                       = ()
  override def forceSync(): Int                  = 0
  override def forceSync(policyId: ByteStr): Int = 0
  override def close(): Unit                     = ()
}

/**
  * Observes policy updates and synchronizes privacy storage with blockchain state. This process consists of two
  * streams: crawling and rollbacks.
  *
  * Crawling stream requests data from peers with the specified parallelism parameter that ensures back pressure.
  * This stream consists of three sub-streams: lost items processing, pending items processing and blockchain updates
  * processing. The first two sub-streams have their own back pressure to ensure that the synchronizer can always
  * process new blockchain items.
  *
  * Rollbacks stream just cancels running crawler tasks.
  *
  * {{{
  *                            +-------------------------------------------+
  *                            | Synchronization                           |
  *                            |                                           |
  *                            |  +-------------------------------------+  |
  *                            |  | Crawling                            |  |
  *                            |  |                                     |  |
  *  +--------------+          |  |  +-------------------------------+  |  |
  *  |              | ~~~~~~~~~~~~~> | updates (100% of parallelism) |  |  |
  *  |              |          |  |  +-------------------------------+  |  |
  *  |              |          |  |                                     |  |
  *  |              |          |  |  +-------------------------------+  |  |
  *  |              |          |  |  | pending (50% of parallelism)  |  |  |
  *  |              |          |  |  +-------------------------------+  |  |
  *  |              |          |  |                                     |  |
  *  |  Blockchain  |          |  |  +-------------------------------+  |  |
  *  |              |          |  |  | lost (25% of parallelism)     |  |  |
  *  |              |          |  |  +-------------------------------+  |  |
  *  |              |          |  +-------------------------------------+  |
  *  |              |          |                                           |
  *  |              |          |  +-------------------------------------+  |
  *  |              | ~~~~~~~~~~> |              rollbacks              |  |
  *  +--------------+          |  +-------------------------------------+  |
  *                            +-------------------------------------------+
  * }}}
  */
class EnablePolicyDataSynchronizer(
    protected val state: Blockchain with BlockchainUpdater with PrivacyState,
    owner: PrivateKeyAccount,
    settings: PrivacySynchronizerSettings,
    responses: ChannelObservable[PrivateDataResponse],
    privacyInventoryHandler: PrivacyInventoryHandler,
    protected val peers: ActivePeerConnections,
    maxSimultaneousConnections: Int,
    protected val storage: PolicyStorage,
    strictDataCache: PolicyStrictDataCache,
    time: Time
)(implicit scheduler: Scheduler)
    extends PolicyDataSynchronizer
    with PolicyInventoryBroadcaster
    with ScorexLogging
    with AutoCloseable {

  private type FibersMap = collection.concurrent.Map[PolicyDataId, Fiber[Unit]]

  private[this] val synchronization = SerialCancelable()

  /* Blocks processing of several streams within one channel */
  private[this] val channelStreamLoadingLocks =
    CacheBuilder
      .newBuilder()
      .maximumSize(maxSimultaneousConnections)
      .expireAfterAccess(settings.requestTimeout.toMillis * 3, TimeUnit.MILLISECONDS)
      .build[ChannelId, Semaphore[Task]](new CacheLoader[ChannelId, Semaphore[Task]] {
        override def load(key: ChannelId): Semaphore[Task] = Semaphore.unsafe[Task](1)
      })

  import EnablePolicyDataSynchronizer._
  import PolicyDataSynchronizerError._

  def run(): Unit = {
    log.debug("Run policy data synchronizer")
    internalRun()
  }

  private[network] def internalRun(): FibersMap =
    runSynchronization(state.pendingPrivacyItems())

  def forceSync(): Int = {
    log.debug("Attempting force sync")
    val (_, restartedCount) = internalForceSync()
    restartedCount
  }

  private[network] def internalForceSync(): (FibersMap, Int) = {
    val pending = state.pendingPrivacyItems()
    runSynchronization(pending) -> pending.size
  }

  def forceSync(policyId: ByteStr): Int = {
    log.debug(s"Attempting force sync for policyId: '$policyId'")
    val (_, restartedCount) = internalForceSync(policyId)
    restartedCount
  }

  private[network] def internalForceSync(policyId: ByteStr): (FibersMap, Int) = {
    val pending          = state.pendingPrivacyItems()
    val fromBlockChain   = state.policyDataHashes(policyId).map(dataHash => PolicyDataId(policyId, dataHash))
    val maybeMissedTasks = fromBlockChain -- pending
    if (maybeMissedTasks.nonEmpty) {
      val added = state.addToPending(maybeMissedTasks)
      PrivacyMetrics.pendingSizeStats.increment(added)
      log.trace(s"Recovered missed requests: ${maybeMissedTasks.mkString("[", ", ", "]")}")
    }
    val pendingIds = pending ++ fromBlockChain
    runSynchronization(pendingIds) -> pendingIds.size
  }

  override def close(): Unit = synchronization.cancel()

  private def runSynchronization(initPendingIds: Set[PolicyDataId]): FibersMap = {
    log.info(s"Start policy data synchronizer with '${initPendingIds.size}' pending items")
    PrivacyMetrics.pendingSizeStats.increment(initPendingIds.size)

    val crawlingFibers = TrieMap.empty[PolicyDataId, Fiber[Unit]]

    val crawlingStream = buildCrawlingStream(state.policyUpdates, crawlingFibers, initPendingIds)
    val rollbackStream = buildRollbackStream(state.policyRollbacks, crawlingFibers)

    synchronization := Observable(crawlingStream, rollbackStream).merge
      .guarantee {
        Task {
          val pendingSize = state.pendingPrivacyItems().size
          log.info(s"Policy data synchronizer stops, pending tasks count '$pendingSize'")
          PrivacyMetrics.pendingSizeStats.decrement(pendingSize)
        } >> crawlingFibers.values.map(_.cancel).toList.parSequence_
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

    crawlingFibers
  }

  private def buildCrawlingStream(policyUpdates: Observable[PolicyUpdate],
                                  crawlingFibers: FibersMap,
                                  pendingIds: Iterable[PolicyDataId]): Observable[Unit] = {
    Observable
      .fromTask(Semaphore[Task](settings.crawlingParallelism.value))
      .flatMap { crawlingSemaphore =>
        val lostDataSubStream      = buildLostDataSubStream(crawlingSemaphore, crawlingFibers)
        val pendingDataSubStream   = buildPendingDataSubStream(pendingIds, crawlingSemaphore, crawlingFibers)
        val policyUpdatesSubStream = buildPolicyUpdatesSubStream(policyUpdates, crawlingSemaphore, crawlingFibers)

        Observable(lostDataSubStream, pendingDataSubStream, policyUpdatesSubStream).merge
      }
  }

  private def buildRollbackStream(policyRollbacks: Observable[PolicyDataId], crawlingFibers: FibersMap): Observable[Unit] = {
    policyRollbacks
      .asyncBoundary(OverflowStrategy.Default)
      .mapEval { key =>
        Task(log.debug(s"Rollback for policy '${key.policyId}' data '${key.dataHash}'")) *>
          crawlingFibers.get(key).fold(Task.unit)(_.cancel)
      }
  }

  private def buildLostDataSubStream(crawlingSemaphore: Semaphore[Task], crawlingFibers: FibersMap): Observable[Unit] = {
    val lostProcessingParallelism = math.max(1, settings.crawlingParallelism.value / 4)

    def forever(lostProcessingSemaphore: Semaphore[Task]): Observable[Unit] =
      Observable(
        Observable
          .fromIterable {
            val lostItems = Random.shuffle(state.lostPrivacyItems().toSeq)
            if (lostItems.nonEmpty) log.warn(s"Started lost items '${lostItems.mkString("', '")}' crawling")
            lostItems
          }
          .mapEval { lostItemKey =>
            lostProcessingSemaphore.acquire >>
              crawl(crawlingSemaphore, crawlingFibers, lostItemKey, externalGuarantee = lostProcessingSemaphore.release)
          },
        Observable
          .defer {
            // Wait until the entire queue of lost items has been processed and start over
            Observable.fromTask(lostProcessingSemaphore.awaitAvailable(lostProcessingParallelism)) >>
              forever(lostProcessingSemaphore)
          }
          .delayExecution(settings.lostDataProcessingDelay)
      ).concat

    Observable
      .fromTask(Semaphore[Task](lostProcessingParallelism))
      .flatMap(forever)
  }

  private def buildPendingDataSubStream(pendingIds: Iterable[PolicyDataId],
                                        crawlingSemaphore: Semaphore[Task],
                                        crawlingFibers: FibersMap): Observable[Unit] = {
    val pendingProcessingParallelism = math.max(1, settings.crawlingParallelism.value / 2)

    Observable
      .fromTask(Semaphore[Task](pendingProcessingParallelism))
      .flatMap { pendingSemaphore =>
        Observable
          .fromIterable(Random.shuffle(pendingIds.toSeq))
          .mapEval { pendingItemKey =>
            pendingSemaphore.acquire >>
              crawl(crawlingSemaphore, crawlingFibers, pendingItemKey, externalGuarantee = pendingSemaphore.release)
          }
      }
  }

  private def buildPolicyUpdatesSubStream(policyUpdates: Observable[PolicyUpdate],
                                          crawlingSemaphore: Semaphore[Task],
                                          crawlingFibers: FibersMap): Observable[Unit] = {
    policyUpdates
      .asyncBoundary(OverflowStrategy.Default)
      .mapEval {
        case PolicyUpdate(key, maybeTxTs) =>
          crawl(crawlingSemaphore, crawlingFibers, key, maybeTxTs)
      }
  }

  private def crawl(crawlingSemaphore: Semaphore[Task],
                    crawlingFibers: FibersMap,
                    key: PolicyDataId,
                    maybeTxTimestamp: Option[Long] = None,
                    externalGuarantee: Task[Unit] = Task.unit): Task[Unit] = {

    def removeFiber: Task[Unit] = Task {
      log.trace(s"Remove crawling fiber for policy '${key.policyId}' data '${key.dataHash}'")
      crawlingFibers.remove(key)
      PrivacyMetrics.crawlingParallelismStats.decrement()
    }

    def addFiber(fiber: Fiber[Unit]): Task[Unit] = Task {
      log.trace(s"Create new crawling fiber for policy '${key.policyId}' data '${key.dataHash}'")
      crawlingFibers.put(key, fiber)
      PrivacyMetrics.crawlingParallelismStats.increment()
    }

    crawlingSemaphore.acquire *> ensureDataInStorage(key.policyId, key.dataHash, maybeTxTimestamp)
      .guarantee {
        (crawlingSemaphore.release, externalGuarantee, removeFiber).parTupled.void
      }
      .start
      .flatMap(addFiber)
  }

  private def ensureDataInStorage(policyId: ByteStr, dataHash: PolicyDataHash, maybeTxTimestamp: Option[Long]): Task[Unit] =
    Task
      .defer {
        val start = System.currentTimeMillis()
        storage.policyItemExists(policyId.toString, dataHash.stringRepr).flatMap {
          case Left(error) =>
            Task.raiseError(StorageUnavailable(error))
          case Right(exists) =>
            (if (exists) {
               Task(log.debug(s"Data for policy '$policyId' with dataHash '$dataHash' already in storage"))
             } else {
               for {
                 _        <- Task(log.debug(s"Start data crawling for policy '$policyId' data '$dataHash'"))
                 dataType <- pullFromPeers(policyId, dataHash, settings.initRetryDelay)
                 _        <- Task(PrivacyMetrics.writeRawTime(SynchronizerCrawling, policyId.toString, dataHash.toString, start))
                 _        <- Task(state.putItemDescriptor(policyId, dataHash, PrivacyItemDescriptor(dataType)))
                 _        <- buildAndBroadcastInventoryIfNeeded(policyId, dataHash, dataType, maybeTxTimestamp)
               } yield ()
             }) *> Task {
              val (isRemovedFromPending, _) = state.removeFromPendingAndLost(policyId, dataHash)
              if (isRemovedFromPending) PrivacyMetrics.pendingSizeStats.decrement()
              log.debug(s"Completed data crawling for policy '$policyId' data '$dataHash'")
            }
        }
      }
      .onErrorHandleWith { ex =>
        Task(log.warn(s"Failed to ensure the availability of policy '$policyId' data '$dataHash' in the storage", ex))
      }

  private def buildAndBroadcastInventoryIfNeeded(policyId: ByteStr,
                                                 dataHash: PolicyDataHash,
                                                 dataType: PrivacyDataType,
                                                 maybeTxTimestamp: Option[Long]): Task[Unit] =
    Task.defer {
      if (maybeTxTimestamp.exists(txTs => time.correctedTime() - txTs < settings.inventoryTimestampThreshold.toMillis)) {
        buildPrivacyInventory(dataType, policyId, dataHash, owner)
          .map { inventory =>
            broadcastInventory(inventory)
            log.debug(s"Policy '$policyId' data '$dataHash' inventory $inventory has been sent")
          }
      } else {
        Task {
          log.debug(
            s"Policy '$policyId' data '$dataHash' inventory is not generated because " +
              maybeTxTimestamp.fold("target timestamp is undefined")(txTs => s"target timestamp '$txTs' is too old")
          )
        }
      }
    }

  private def pullFromPeers(policyId: ByteStr,
                            dataHash: PolicyDataHash,
                            retryDelay: FiniteDuration,
                            attemptsCount: Int = 1,
                            withInventoryRequest: Boolean = false): Task[PrivacyDataType] =
    Task.defer {
      val recipients    = state.policyRecipients(policyId)
      val dataId        = PolicyDataId(policyId, dataHash)
      val selectedPeers = selectPeers(recipients, policyId)

      if (selectedPeers.nonEmpty) {
        val maybeInventoryRequest =
          if (withInventoryRequest || !privacyInventoryHandler.containsInventoryDataOwners(dataId)) {
            sendToPeers(PrivacyInventoryRequest(policyId, dataHash), selectedPeers: _*) *>
              Task {
                log.debug(s"Inventory request for policy '$policyId' data '$dataHash'")
                PrivacyMetrics.writeEvent(SynchronizerInventoryRequest, dataId.policyId.toString, dataId.dataHash.toString)
              }.delayResult(settings.inventoryRequestDelay)
          } else {
            Task(log.trace(s"No need to request inventory for policy '$policyId' data '$dataHash'"))
          }

        maybeInventoryRequest *> pullByInventory(recipients, dataId, attemptsCount, retryDelay)
      } else {
        pullFallback(policyId, dataHash, attemptsCount, retryDelay)
      }
    }

  protected def selectPeers(policyRecipients: Set[Address], policyId: ByteStr): Vector[PeerSession] = {
    val policyPeerSessions = peers.withAddresses(policyRecipients.contains, excludeWatchers = true).toVector

    if (policyPeerSessions.isEmpty) {
      log.warn(s"Couldn't find participants of policy '$policyId' among connected peers, scheduling retry")
    }

    policyPeerSessions
  }

  private def pullFallback(policyId: ByteStr,
                           dataHash: PolicyDataHash,
                           pullAttemptsCount: Int,
                           retryDelay: FiniteDuration,
                           withInventoryRequest: Boolean = false): Task[PrivacyDataType] =
    Task.defer {
      if (pullAttemptsCount + 1 > settings.maxAttemptCount.value) {
        Task.defer {
          val (isRemovedFromPending, _) = state.pendingToLost(policyId, dataHash)
          if (isRemovedFromPending) PrivacyMetrics.pendingSizeStats.decrement()
          val error = LostDataException(policyId, dataHash)
          log.warn(error.message)
          Task.raiseError(error)
        }
      } else {
        val task =
          Task(PrivacyMetrics.writeEvent(SynchronizerPullFallback, policyId.toString, dataHash.toString)) *>
            pullFromPeers(
              policyId = policyId,
              dataHash = dataHash,
              retryDelay = (retryDelay.toMillis * PullRetryDelayFactor).millis,
              attemptsCount = pullAttemptsCount + 1,
              withInventoryRequest = withInventoryRequest,
            )

        task.delayExecution(retryDelay)
      }
    }

  import scala.collection.JavaConverters._

  private val failedAddresses =
    CacheBuilder
      .newBuilder()
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build[Address, Unit]()
      .asMap()
      .asScala

  private def pullByInventory(policyRecipients: Set[Address],
                              dataId: PolicyDataId,
                              pullAttemptsCount: Int,
                              retryDelay: FiniteDuration): Task[PrivacyDataType] = {
    import dataId.{dataHash, policyId}
    val coarsestRetryDelay = retryDelay.toSeconds.seconds.toCoarsest

    privacyInventoryHandler
      .inventoryObservable(dataId)
      .flatMap { inventoryDescriptors =>
        val filteredDescriptors = inventoryDescriptors.filterNot(desc => failedAddresses.contains(desc.senderAddress))
        val descriptorsToProcess = if (filteredDescriptors.nonEmpty) {
          filteredDescriptors
        } else {
          failedAddresses.clear()
          inventoryDescriptors
        }
        val descriptors = Random.shuffle(descriptorsToProcess.toSeq)
        log.trace(s"Going to process the following descriptors: [${descriptors}]")
        Observable.fromIterable(descriptors)
      }
      .asyncBoundary(OverflowStrategy.Default)
      .timeoutOnSlowUpstream(settings.inventoryStreamTimeout)
      .flatMap { inventoryDescriptor =>
        log.trace(s"Process $inventoryDescriptor")
        val inventoryAddress          = inventoryDescriptor.senderAddress
        val validatedInventoryAddress = policyRecipients.find(_ == inventoryAddress)
        Observable.fromIterable {
          peers
            .withAddresses(validatedInventoryAddress.contains, excludeWatchers = true)
            .map(_ -> inventoryDescriptor.dataType)
        }
      }
      .zipWithIndex
      .mapEval {
        case ((session, dataType), i) =>
          requestAndProcessData(policyId, dataHash, session, dataType)
            .as(Some(dataType))
            .onErrorRecover {
              case NonFatal(error) =>
                log.warn(
                  s"Failed to retrieve response for policy '$policyId' data '$dataHash' with address '${session.peerInfo.nodeOwnerAddress}'" +
                    s" and channel '${id(session.channel)}'",
                  error
                )
                None
            }
            .flatTap { _ =>
              Task {
                if (i > 0) PrivacyMetrics.writeRawNumber(SynchronizerInventoryIteration, policyId.toString, dataHash.toString, "count" -> i.toInt)
              }
            }
      }
      .flatMap(Observable.fromIterable(_))
      .firstL
      .onErrorRecoverWith {
        case _: UpstreamTimeoutException =>
          Task(log.warn(s"Slow inventory upstream for policy '$policyId' data '$dataHash', retrying after $coarsestRetryDelay")) *>
            pullFallback(dataId.policyId, dataId.dataHash, pullAttemptsCount, retryDelay, withInventoryRequest = true)
        case NonFatal(ex) =>
          Task {
            log.error(
              s"Failed to pull data by inventory for policy '$policyId' with data '$dataHash', retrying after $coarsestRetryDelay",
              ex
            )
          } *> pullFallback(dataId.policyId, dataId.dataHash, pullAttemptsCount, retryDelay)
      }
  }

  private def requestAndProcessData(policyId: ByteStr, dataHash: PolicyDataHash, peer: PeerSession, dataType: PrivacyDataType): Task[Unit] = {
    def startAwaitingAndBuildProcessingTask: Task[Task[Unit]] =
      dataType match {
        case PrivacyDataType.Large =>
          awaitStreamResponse(policyId, dataHash, peer.channel).map(processResponseStream(policyId, dataHash, peer.channel, _))
        case PrivacyDataType.Default =>
          awaitStrictResponse(policyId, dataHash, peer.channel).map(processStrictResponse(policyId, dataHash, peer.channel, _))
      }

    (for {
      processingTask <- startAwaitingAndBuildProcessingTask
      _              <- sendToPeers(PrivateDataRequest(policyId, dataHash), peer)
      _ <- processingTask.onError {
        case _: UpstreamTimeoutException => Task(failedAddresses.put(peer.address, ())).void
      }
    } yield ()).timeout(settings.requestTimeout)
  }

  private def awaitStreamResponse(policyId: ByteStr, dataHash: PolicyDataHash, channel: Channel): Task[Observable[Array[Byte]]] =
    Task.defer {
      val listener = new StreamReadProgressListener(s"${id(channel)} policy '$policyId' data '$dataHash' loading")
      val handler  = new PrivacyDataStreamHandler(channel, listener, settings.networkStreamBufferSize.value)
      channelStreamLoadingLocks.get(channel.id()).withPermit {
        Task {
          channel.pipeline().addBefore(MetaMessageCodecHandlerName, PrivacyDataStreamHandler.Name, handler)
          handler.dataStream.timeoutOnSlowUpstream(settings.requestTimeout).guarantee(Task(handler.dispose()))
        }
      }
    }

  private def awaitStrictResponse(policyId: ByteStr, dataHash: PolicyDataHash, channel: Channel): Task[Fiber[NonEmptyResponse]] = {
    responses
      .asyncBoundary(OverflowStrategy.Default)
      .filter { case (ch, response) => ch == channel && policyId == response.policyId && response.dataHash == dataHash }
      .mapEval {
        case (_, response: NonEmptyResponse) =>
          Task.pure(response)
        case _ =>
          log.debug(s"Peer '${id(channel)}' have no data for policy '$policyId' data hash '$dataHash'")
          Task.raiseError(DataNotFoundError(policyId, dataHash))
      }
      .timeoutOnSlowUpstream(settings.requestTimeout)
      .firstL
      .start
  }

  protected def sendToPeers(request: Object, peers: PeerSession*): Task[Unit] =
    peers.view
      .map { session =>
        taskFromChannelFuture(session.channel.writeAndFlush(request)).map { _ =>
          log.trace(s"Sent $request to channel '${id(session.channel)}' with address '${session.address}'")
        }
      }
      .toList
      .parSequence_

  private def processResponseStream(policyId: ByteStr, dataHash: PolicyDataHash, channel: Channel, input: Observable[Array[Byte]]): Task[Unit] = {
    import StreamResponse._

    def saveToDb(dataLoadingStart: StartDataLoadingEvent, dataStream: Observable[Byte]): Task[Unit] =
      EitherT {
        storage.savePolicyDataWithMeta(Right(dataStream), dataLoadingStart.metaData)
      } valueOrF {
        case EntityAlreadyExists => Task(log.debug(s"Stream data for policy '$policyId' with dataHash '$dataHash' is already in the storage"))
        case error               => Task.raiseError(StorageUnavailable(error))
      }

    for {
      _                <- Task(log.trace(s"Process response stream for policy '$policyId' data '$dataHash'"))
      peerConnection   <- Task.fromEither(peers.peerConnection(channel).toRight(NoPeerInfo(channel)))
      processingSetup  <- buildStreamProcessingSetup(peerConnection, policyId, dataHash)
      processingFiber  <- (input.filter(_.nonEmpty) :+ Terminator).consumeWith(processingSetup.consumer).start
      dataLoadingStart <- processingSetup.startAwaiting
      dataStream = dataLoadingStart.dataStream.flatMap(Observable.fromIterable(_))
      _ <- saveToDb(dataLoadingStart, dataStream) <& processingSetup.endAwaiting
      _ <- processingFiber.join
    } yield ()
  }

  private def processStrictResponse(policyId: ByteStr,
                                    dataHash: PolicyDataHash,
                                    channel: Channel,
                                    responseAwaiting: Fiber[NonEmptyResponse]): Task[Unit] = {
    import StrictResponse._

    responseAwaiting.join
      .flatMap {
        case GotEncryptedDataResponse(_, _, encryptedData) => decryptStrictResponse(policyId, dataHash, peers, channel, encryptedData)
        case GotDataResponse(_, _, data)                   => Task.fromEither(deserializeMetaDataAndData(data.arr))
      }
      .flatMap { strictResponse =>
        log.debug(s"Saving strict data for policy '$policyId' with dataHash '$dataHash'")

        EitherT {
          PrivacyMetrics.measureTask(SynchronizerDataSaving, policyId.toString, dataHash.toString) {
            storage.savePolicyDataWithMeta(Left(strictResponse.data), strictResponse.metaData)
          }
        } valueOrF {
          case EntityAlreadyExists => Task(log.debug(s"Strict data for policy '$policyId' with dataHash '$dataHash' is already in the storage"))
          case error               => Task.raiseError(StorageUnavailable(error))
        } guarantee {
          Task(strictDataCache.invalidate(PolicyDataId(policyId, dataHash)))
        }
      }
  }
}

object EnablePolicyDataSynchronizer extends ScorexLogging {

  val PullRetryDelayFactor: Double = 4.0 / 3

  case class StrictResponse(metaData: PolicyMetaData, data: ByteStr)

  object StrictResponse {

    def decryptStrictResponse(policyId: ByteStr,
                              dataHash: PolicyDataHash,
                              peers: ActivePeerConnections,
                              channel: Channel,
                              encrypted: EncryptedForSingle): Task[StrictResponse] = Task.defer {
      log.trace(s"Decrypting strict response for policy '$policyId' data '$dataHash'")

      Task.fromEither {
        PrivacyMetrics.measureEither(SynchronizerDataDecrypting, policyId.toString, dataHash.toString) {
          for {
            peerConnection <- peers.peerConnection(channel).toRight(NoPeerInfo(channel))
            peerPublicKey = peerConnection.peerInfo.sessionPubKey.publicKey
            decryptedBytes <- crypto
              .decrypt(encrypted, peerConnection.sessionKey.privateKey, peerPublicKey)
              .leftMap(cause => DecryptionError(policyId, dataHash, cause))
            strictResponse <- deserializeMetaDataAndData(decryptedBytes)
          } yield strictResponse
        }
      }
    }

    def deserializeMetaDataAndData(bytes: Array[Byte]): Either[DeserializingError, StrictResponse] = {
      val byteBuf    = ByteBuffer.wrap(bytes)
      val dataLength = byteBuf.getInt()
      val dataBytes  = new Array[Byte](dataLength)
      byteBuf.get(dataBytes)

      val metaBytes = new Array[Byte](byteBuf.remaining())
      byteBuf.get(metaBytes)

      PolicyMetaData
        .fromBytes(metaBytes)
        .toEither
        .bimap(cause => DeserializingError(ByteStr(bytes), cause), metaData => StrictResponse(metaData, ByteStr(dataBytes)))
    }
  }

  //noinspection UnstableApiUsage
  object StreamResponse {

    val Terminator: Array[Byte]        = Array.empty
    val ExistenceHeaderSize: Int       = 1
    val MetaDataLengthSize: Int        = Integer.BYTES
    val EncryptionChunkLengthSize: Int = Integer.BYTES

    sealed trait StreamProcessingAccumulator

    object StreamProcessingAccumulator {
      case object Empty extends StreamProcessingAccumulator

      case class HeaderAccumulation(out: ByteArrayDataOutput, written: Int) extends StreamProcessingAccumulator

      case class EncryptedMetadataAccumulation(out: ByteArrayDataOutput, size: Int, decryptor: StreamCipher.AbstractDecryptor, metadataLength: Int)
          extends StreamProcessingAccumulator

      case class RawMetadataAccumulation(out: ByteArrayDataOutput, written: Int, metadataLength: Int) extends StreamProcessingAccumulator

      case class EncryptedDataStreaming(decryptor: StreamCipher.AbstractDecryptor, dataOutputQueue: MVar[Task, Array[Byte]])
          extends StreamProcessingAccumulator

      case class RawDataStreaming(dataOutputQueue: MVar[Task, Array[Byte]]) extends StreamProcessingAccumulator

      case object Completed extends StreamProcessingAccumulator
    }

    case class StartDataLoadingEvent(metaData: PolicyMetaData, dataStream: Observable[Array[Byte]])

    case class StreamProcessingSetup(startAwaiting: Task[StartDataLoadingEvent],
                                     endAwaiting: Task[Unit],
                                     consumer: Consumer[Array[Byte], StreamProcessingAccumulator])

    import StreamProcessingAccumulator._

    def buildStreamProcessingSetup(
        peer: PeerConnection,
        policyId: ByteStr,
        dataHash: PolicyDataHash
    )(implicit scheduler: Scheduler): Task[StreamProcessingSetup] =
      Task.defer {
        val withEncryption = !peer.channel.hasAttr(TlsAttribute)

        val startPromise = Promise[StartDataLoadingEvent]()
        val startTask    = Task.deferFuture(startPromise.future)

        val endPromise = Promise[Unit]()
        val endTask    = Task.deferFuture(endPromise.future)

        val streamConsumer = Consumer.foldLeftTask[StreamProcessingAccumulator, Array[Byte]](Empty) {
          case (accState, chunk) =>
            Task
              .defer {
                accState match {
                  case Empty =>
                    processFirstChunk(policyId, dataHash, chunk, peer, startPromise, withEncryption)
                  case HeaderAccumulation(out, written) =>
                    accumulateHeader(policyId, dataHash, chunk, written, out, peer, startPromise, withEncryption)
                  case EncryptedMetadataAccumulation(out, written, decryptor, metaDataLength) =>
                    accumulateMetaData(policyId, dataHash, out, written, chunk, startPromise, endPromise, Some(decryptor), metaDataLength)
                  case RawMetadataAccumulation(out, written, metaDataLength) =>
                    accumulateMetaData(policyId, dataHash, out, written, chunk, startPromise, endPromise, None, metaDataLength)
                  case EncryptedDataStreaming(decryptor, queue) =>
                    accumulateEncryptedData(policyId, dataHash, decryptor, queue, chunk, endPromise)
                  case RawDataStreaming(queue) =>
                    accumulateRawData(policyId, dataHash, queue, chunk, endPromise)
                  case Completed =>
                    Task.raiseError(MalformedStreamError(policyId, dataHash))
                }
              }
              .onErrorRecoverWith {
                case NonFatal(ex) =>
                  Task {
                    if (!startPromise.isCompleted) startPromise.failure(ex)
                    if (!endPromise.isCompleted) endPromise.failure(ex)
                  } *> Task.raiseError(ex)
              }

        }

        Task.pure(StreamProcessingSetup(startTask, endTask, streamConsumer))
      }

    private def processFirstChunk(policyId: ByteStr,
                                  dataHash: PolicyDataHash,
                                  chunk: Array[Byte],
                                  connection: PeerConnection,
                                  startPromise: Promise[StartDataLoadingEvent],
                                  withEncryption: Boolean)(implicit scheduler: Scheduler): Task[StreamProcessingAccumulator] =
      Task.defer {
        val WrappedStructureLength = crypto.context.algorithms.WrappedStructureLength

        log.trace(s"Process first '${chunk.length}' bytes chunk")

        val requiredHeaderSize = {
          if (withEncryption)
            ExistenceHeaderSize + EncryptionChunkLengthSize + WrappedStructureLength + MetaDataLengthSize
          else
            ExistenceHeaderSize + MetaDataLengthSize
        }

        if (chunk.length >= requiredHeaderSize) {
          if (withEncryption) {
            processFirstChunkWithEncryption(policyId, dataHash, chunk, connection, startPromise)
          } else {
            processFirstChunkWithoutEncryption(policyId, dataHash, chunk, startPromise)
          }
        } else {
          Task {
            val out = newDataOutput()
            out.write(chunk)
            log.trace(
              s"Next stage for policy '$policyId' data '$dataHash' is 'Header ($requiredHeaderSize) accumulation'" +
                s" with '${chunk.length}' bytes"
            )
            HeaderAccumulation(out, chunk.length)
          }
        }
      }

    private def processFirstChunkWithEncryption(policyId: ByteStr,
                                                dataHash: PolicyDataHash,
                                                chunk: Array[Byte],
                                                connection: PeerConnection,
                                                startPromise: Promise[StartDataLoadingEvent]): Task[StreamProcessingAccumulator] = {
      val WrappedStructureLength = crypto.context.algorithms.WrappedStructureLength

      val (chunkSizeBytes, chunkSizeRest)          = chunk.view.tail.splitAt(EncryptionChunkLengthSize)
      val chunkSize                                = Ints.fromByteArray(chunkSizeBytes.toArray)
      val (wrappedStructure, wrappedStructureRest) = chunkSizeRest.splitAt(WrappedStructureLength)

      crypto.context.algorithms.buildDecryptor(
        wrappedStructure.toArray,
        connection.sessionKey.privateKey,
        connection.peerInfo.sessionPubKey.publicKey,
        chunkSize
      ) match {
        case Left(err) =>
          Task.raiseError(DecryptionError(policyId, dataHash, err))
        case Right(decryptor) =>
          Task.defer {
            val (metaDataLengthBytes, metaDataLengthRest) = wrappedStructureRest.splitAt(MetaDataLengthSize)
            val metaDataLength                            = Ints.fromByteArray(metaDataLengthBytes.toArray)

            Task
              .fromEither {
                decryptor(metaDataLengthRest.toArray).leftMap(cause => DecryptionError(policyId, dataHash, cause))
              }
              .flatMap { decryptedMetaDataLengthRest =>
                if (decryptedMetaDataLengthRest.length >= metaDataLength) {
                  val (metaDataBytes, metaDataRest) = decryptedMetaDataLengthRest.splitAt(metaDataLength)
                  startDataLoading(startPromise, metaDataBytes, metaDataRest)
                    .map { queue =>
                      log.trace(s"Next stage for policy '$policyId' data '$dataHash' is 'Encrypted data streaming'")
                      EncryptedDataStreaming(decryptor, queue)
                    }
                } else {
                  Task {
                    val out  = newDataOutput()
                    val body = decryptedMetaDataLengthRest
                    out.write(body)
                    log.trace(
                      s"Next stage for policy '$policyId' data '$dataHash' is 'Encrypted metadata ($metaDataLength) accumulation'" +
                        s" with '${body.length}' bytes"
                    )
                    EncryptedMetadataAccumulation(out, body.length, decryptor, metaDataLength)
                  }
                }
              }
          }
      }
    }

    private def processFirstChunkWithoutEncryption(policyId: ByteStr,
                                                   dataHash: PolicyDataHash,
                                                   chunk: Array[Byte],
                                                   startPromise: Promise[StartDataLoadingEvent]): Task[StreamProcessingAccumulator] = {

      val (metaDataLengthBytes, metaDataLengthRest) = chunk.view.tail.splitAt(MetaDataLengthSize)
      val metaDataLength                            = Ints.fromByteArray(metaDataLengthBytes.toArray)

      if (metaDataLengthRest.length >= metaDataLength) {
        val (metaDataBytes, metaDataRest) = metaDataLengthRest.splitAt(metaDataLength)
        startDataLoading(startPromise, metaDataBytes.toArray, metaDataRest.toArray)
          .map { queue =>
            log.trace(s"Next stage for policy '$policyId' data '$dataHash' is 'Raw data streaming'")
            RawDataStreaming(queue)
          }
      } else {
        Task {
          val out  = newDataOutput()
          val body = metaDataLengthRest.toArray
          out.write(body)
          log.trace(
            s"Next stage for policy '$policyId' data '$dataHash' is 'Raw metadata ($metaDataLength) accumulation'" +
              s" with '$metaDataLength' bytes"
          )
          RawMetadataAccumulation(out, body.length, metaDataLength)
        }
      }
    }

    private def startDataLoading(startPromise: Promise[StartDataLoadingEvent],
                                 metaDataBytes: Array[Byte],
                                 rest: Array[Byte],
                                 isLast: Boolean = false): Task[MVar[Task, Array[Byte]]] = {
      for {
        metaData <- Task.fromTry(PolicyMetaData.fromBytes(metaDataBytes))
        queue    <- MVar.empty[Task, Array[Byte]]()
        _        <- if (rest.nonEmpty) queue.put(rest) else Task.unit
        stream = Observable.repeatEvalF(queue.take).takeWhile(_.nonEmpty)
        _      = log.trace(s"Start stream data loading for policy '${metaData.policyId}' data '${metaData.hash}' with first chunk '${rest.length}'")
        _      = startPromise.success(StartDataLoadingEvent(metaData, stream))
        _ <- if (isLast) queue.put(Terminator) else Task.unit
      } yield queue
    }

    private def accumulateHeader(policyId: ByteStr,
                                 dataHash: PolicyDataHash,
                                 chunk: Array[Byte],
                                 written: Int,
                                 out: ByteArrayDataOutput,
                                 connection: PeerConnection,
                                 startPromise: Promise[StartDataLoadingEvent],
                                 withEncryption: Boolean): Task[StreamProcessingAccumulator] = Task.defer {

      val isLastChunk = chunk.isEmpty

      if (isLastChunk) {
        log.trace(s"Got the last chunk on header accumulation")
      } else {
        log.trace(s"Accumulate header with '${chunk.length}' bytes chunk")
      }

      out.write(chunk)
      val updatedWritten = written + chunk.length

      if (isLastChunk) {
        out.toByteArray.headOption match {
          case Some(PolicyDataStreamResponse.DataNotFound.value)    => Task.raiseError(DataNotFoundError(policyId, dataHash))
          case Some(PolicyDataStreamResponse.TooManyRequests.value) => Task.raiseError(PeerOverloadedError(connection.channel, policyId, dataHash))
          case _                                                    => Task.raiseError(MalformedStreamError(policyId, dataHash))
        }
      } else {
        if (withEncryption) {
          accumulateHeaderWithEncryption(policyId, dataHash, out, connection, updatedWritten, startPromise)
        } else {
          accumulateHeaderWithoutEncryption(policyId, dataHash, out, startPromise, updatedWritten)
        }
      }
    }

    private def accumulateHeaderWithEncryption(policyId: ByteStr,
                                               dataHash: PolicyDataHash,
                                               out: ByteArrayDataOutput,
                                               connection: PeerConnection,
                                               written: Int,
                                               startPromise: Promise[StartDataLoadingEvent]): Task[StreamProcessingAccumulator] = {
      val WrappedStructureLength = com.wavesenterprise.crypto.context.algorithms.WrappedStructureLength

      val requiredHeaderSize = ExistenceHeaderSize + EncryptionChunkLengthSize + WrappedStructureLength + MetaDataLengthSize

      if (written >= requiredHeaderSize) {
        val accumulatedBytes = out.toByteArray

        if (accumulatedBytes.headOption.contains(PolicyDataStreamResponse.HasData.value)) {
          val (chunkSizeBytes, chunkSizeRest)          = accumulatedBytes.view.tail.splitAt(EncryptionChunkLengthSize)
          val chunkSize                                = Ints.fromByteArray(chunkSizeBytes.toArray)
          val (wrappedStructure, wrappedStructureRest) = chunkSizeRest.splitAt(WrappedStructureLength)

          crypto.context.algorithms.buildDecryptor(
            wrappedStructure.toArray,
            connection.sessionKey.privateKey,
            connection.peerInfo.sessionPubKey.publicKey,
            chunkSize
          ) match {
            case Left(err) =>
              Task.raiseError(DecryptionError(policyId, dataHash, err))
            case Right(decryptor) =>
              val (metaDataLengthBytes, metaDataLengthRest) = wrappedStructureRest.splitAt(MetaDataLengthSize)
              val metaDataLength                            = Ints.fromByteArray(metaDataLengthBytes.toArray)

              Task
                .fromEither {
                  decryptor(metaDataLengthRest.toArray).leftMap(cause => DecryptionError(policyId, dataHash, cause))
                }
                .flatMap { decryptedMetaDataLengthRest =>
                  if (decryptedMetaDataLengthRest.length >= metaDataLength) {
                    val (metaDataBytes, rest) = metaDataLengthRest.splitAt(metaDataLength)

                    startDataLoading(startPromise, metaDataBytes.toArray, rest.toArray)
                      .map { queue =>
                        log.trace(s"Next stage for policy '$policyId' data '$dataHash' is 'Encrypted data streaming'")
                        EncryptedDataStreaming(decryptor, queue)
                      }
                  } else {
                    Task {
                      val out  = newDataOutput()
                      val body = decryptedMetaDataLengthRest
                      out.write(body)
                      log.trace(
                        s"Next stage for policy '$policyId' data '$dataHash' is 'Encrypted metadata ($metaDataLength) accumulation'" +
                          s" with '${body.length}' bytes")
                      EncryptedMetadataAccumulation(out, body.length, decryptor, metaDataLength)
                    }

                  }
                }
          }
        } else {
          Task.raiseError(MalformedStreamError(policyId, dataHash))
        }
      } else {
        Task {
          log.trace(
            s"Next stage for policy '$policyId' data '$dataHash' is 'Header ($requiredHeaderSize) accumulation'" +
              s" with '$written' bytes"
          )
          HeaderAccumulation(out, written)
        }
      }
    }

    private def accumulateHeaderWithoutEncryption(policyId: ByteStr,
                                                  dataHash: PolicyDataHash,
                                                  out: ByteArrayDataOutput,
                                                  startPromise: Promise[StartDataLoadingEvent],
                                                  written: Int): Task[StreamProcessingAccumulator] = {
      val requiredHeaderSize = ExistenceHeaderSize + MetaDataLengthSize

      if (written >= requiredHeaderSize) {
        val accumulatedBytes                          = out.toByteArray
        val (metaDataLengthBytes, metaDataLengthRest) = accumulatedBytes.view.tail.splitAt(MetaDataLengthSize)
        val metaDataLength                            = Ints.fromByteArray(metaDataLengthBytes.toArray)

        if (accumulatedBytes.headOption.contains(PolicyDataStreamResponse.HasData.value)) {
          if (metaDataLengthRest.length >= metaDataLength) {
            val (metaDataBytes, rest) = metaDataLengthRest.splitAt(metaDataLength)

            startDataLoading(startPromise, metaDataBytes.toArray, rest.toArray)
              .map { queue =>
                log.trace(s"Next stream stage for policy '$policyId' data '$dataHash' is raw data streaming")
                RawDataStreaming(queue)
              }
          } else {
            Task {
              val out  = newDataOutput()
              val body = metaDataLengthRest.toArray
              out.write(body)
              log.trace(
                s"Next stage for policy '$policyId' data '$dataHash' is 'Raw metadata ($metaDataLength) accumulation'" +
                  s" with '${body.length}' bytes")
              RawMetadataAccumulation(out, body.length, metaDataLength)
            }
          }
        } else {
          Task.raiseError(MalformedStreamError(policyId, dataHash))
        }
      } else {
        Task {
          log.trace(
            s"Next stage for policy '$policyId' data '$dataHash' is 'Header ($requiredHeaderSize) accumulation'" +
              s" with '$written' bytes"
          )
          HeaderAccumulation(out, written)
        }
      }
    }

    private def accumulateMetaData(policyId: ByteStr,
                                   dataHash: PolicyDataHash,
                                   out: ByteArrayDataOutput,
                                   written: Int,
                                   chunk: Array[Byte],
                                   startPromise: Promise[StartDataLoadingEvent],
                                   endPromise: Promise[Unit],
                                   maybeDecryptor: Option[StreamCipher.AbstractDecryptor],
                                   metaDataLength: Int)(implicit scheduler: Scheduler): Task[StreamProcessingAccumulator] = Task.defer {
      val isLastChunk = chunk.isEmpty

      if (isLastChunk) {
        log.trace(s"Got the last chunk on metadata accumulation")
      } else {
        log.trace(s"Accumulate metadata with '${chunk.length}' bytes chunk")
      }

      maybeDecryptor match {
        case Some(decryptor) =>
          Task
            .fromEither {
              (if (isLastChunk) decryptor.doFinal() else decryptor(chunk)).leftMap(cause => DecryptionError(policyId, dataHash, cause))
            }
            .flatMap { decryptedBytes =>
              out.write(decryptedBytes)
              val updatedWritten = written + decryptedBytes.length

              if (updatedWritten >= metaDataLength) {
                val (metaDataBytes, rest) = out.toByteArray.splitAt(metaDataLength)
                startDataLoading(startPromise, metaDataBytes, rest, isLastChunk)
                  .map { queue =>
                    if (isLastChunk) {
                      endPromise.success(())
                      Completed
                    } else {
                      log.trace(s"Next stage for policy '$policyId' data '$dataHash' is encrypted data streaming")
                      EncryptedDataStreaming(decryptor, queue)
                    }
                  }
              } else {
                Task {
                  if (isLastChunk) {
                    endPromise.success(())
                    Completed
                  } else {
                    log.trace(
                      s"Next stage for policy '$policyId' data '$dataHash' is 'Encrypted metadata ($metaDataLength) accumulation'" +
                        s" with '$updatedWritten' bytes"
                    )
                    EncryptedMetadataAccumulation(out, updatedWritten, decryptor, metaDataLength)
                  }
                }
              }
            }
        case None =>
          out.write(chunk)
          val updatedWritten = written + chunk.length

          if (updatedWritten >= metaDataLength) {
            val (metaDataBytes, rest) = out.toByteArray.splitAt(metaDataLength)
            startDataLoading(startPromise, metaDataBytes, rest, isLastChunk)
              .map { queue =>
                if (isLastChunk) {
                  endPromise.success(())
                  Completed
                } else {
                  log.trace(s"Next stage for policy '$policyId' data '$dataHash' is 'Raw data streaming'")
                  RawDataStreaming(queue)
                }
              }
          } else {
            Task {
              if (isLastChunk) {
                endPromise.success(())
                Completed
              } else {
                log.trace(
                  s"Next stage for policy '$policyId' data '$dataHash' is 'Raw metadata ($metaDataLength) accumulation'" +
                    s" with '$updatedWritten' bytes"
                )
                RawMetadataAccumulation(out, updatedWritten, metaDataLength)
              }
            }
          }
      }
    }

    private def accumulateEncryptedData(policyId: ByteStr,
                                        dataHash: PolicyDataHash,
                                        decryptor: StreamCipher.AbstractDecryptor,
                                        queue: MVar[Task, Array[Byte]],
                                        chunk: Array[Byte],
                                        endPromise: Promise[Unit]): Task[StreamProcessingAccumulator] =
      Task.defer {
        val isLastChunk = chunk.isEmpty

        if (isLastChunk) {
          log.trace(s"Got the last chunk on encrypted data streaming")
        } else {
          log.trace(s"Accumulate encrypted data with '${chunk.length}' bytes chunk")
        }

        Task
          .fromEither {
            (if (isLastChunk) decryptor.doFinal() else decryptor(chunk)).leftMap(cause => DecryptionError(policyId, dataHash, cause))
          }
          .flatMap { decrypted =>
            (if (decrypted.nonEmpty) queue.put(decrypted) else Task.unit) *>
              (if (isLastChunk) queue.put(Terminator) else Task.unit) *>
              Task {
                if (isLastChunk) {
                  endPromise.success(())
                  Completed
                } else {
                  log.trace(s"Next stage for policy '$policyId' data '$dataHash' is 'Encrypted data streaming'")
                  EncryptedDataStreaming(decryptor, queue)
                }
              }
          }
      }

    private def accumulateRawData(policyId: ByteStr,
                                  dataHash: PolicyDataHash,
                                  queue: MVar[Task, Array[Byte]],
                                  chunk: Array[Byte],
                                  endPromise: Promise[Unit]): Task[StreamProcessingAccumulator] =
      Task.defer {
        val isLastChunk = chunk.isEmpty

        if (isLastChunk) {
          log.trace(s"Got the last chunk on raw data streaming")
        } else {
          log.trace(s"Accumulate raw data with '${chunk.length}' bytes chunk")
        }

        (if (chunk.nonEmpty) queue.put(chunk) else Task.unit) *>
          (if (isLastChunk) queue.put(Terminator) else Task.unit) *>
          Task {
            if (isLastChunk) {
              endPromise.success(())
              Completed
            } else {
              log.trace(s"Next stage for policy '$policyId' data '$dataHash' is 'Raw data streaming'")
              RawDataStreaming(queue)
            }
          }
      }
  }
}
