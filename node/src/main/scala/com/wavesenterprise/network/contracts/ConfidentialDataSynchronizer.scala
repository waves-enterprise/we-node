package com.wavesenterprise.network.contracts

import cats.implicits._
import com.google.common.cache.CacheBuilder
import com.google.common.primitives.Ints
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.{CryptoError, EncryptedForSingle}
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.network.Attributes.TlsAttribute
import com.wavesenterprise.network.ConfidentialDataResponse.HasDataResponse
import com.wavesenterprise.network._
import com.wavesenterprise.network.contracts.ConfidentialDataSynchronizerError.{DecryptionError, NoPeerInfo}
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerSession}
import com.wavesenterprise.settings.contract.ConfidentialDataSynchronizerSettings
import com.wavesenterprise.state.contracts.confidential.{ConfidentialDataUnit, ConfidentialInput, ConfidentialOutput}
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId}
import com.wavesenterprise.transaction.{BlockchainUpdater, ConfidentialContractDataUpdate}
import com.wavesenterprise.utils.{AsyncLRUCache, ScorexLogging, Time}
import io.netty.channel.Channel
import monix.catnap.Semaphore
import monix.eval.{Fiber, Task}
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import monix.execution.exceptions.UpstreamTimeoutException
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

sealed abstract class ConfidentialDataSynchronizerError(val message: String, val maybeCause: Option[Throwable] = None)
    extends RuntimeException(message, maybeCause.orNull)

object ConfidentialDataSynchronizerError {
  case class NoPeerInfo(ch: Channel) extends ConfidentialDataSynchronizerError(s"No peer info for channel '${id(ch)}'")

  case class DataNotFoundError(dataId: ConfidentialDataId)
      extends ConfidentialDataSynchronizerError(s"Data for $dataId not found")
  case class LostDataException(dataId: ConfidentialDataId)
      extends ConfidentialDataSynchronizerError(s"Data $dataId marked as lost")

  case class DecryptionError(dataId: ConfidentialDataId, cause: CryptoError)
      extends ConfidentialDataSynchronizerError(s"Failed to decrypt data for $dataId: ${cause.message}")

  case class DeserializingError(bytes: ByteStr, cause: Throwable)
      extends ConfidentialDataSynchronizerError(s"Error while deserializing bytes: '$bytes'", Some(cause))

  case class CustomError(msg: String) extends ConfidentialDataSynchronizerError(msg)
}

/**
  * Observes confidential data updates and synchronizes confidential storage with blockchain state. This process consists of two
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
class ConfidentialDataSynchronizer(
    protected val blockchain: Blockchain with BlockchainUpdater,
    owner: PrivateKeyAccount,
    settings: ConfidentialDataSynchronizerSettings,
    responses: ChannelObservable[ConfidentialDataResponse],
    confidentialDataInventoryHandler: ConfidentialDataInventoryHandler,
    protected val peers: ActivePeerConnections,
    protected val storage: ConfidentialRocksDBStorage,
    dataCache: AsyncLRUCache[ConfidentialDataRequest, ConfidentialDataUnit],
    confidentialDataUpdatesFromUtx: Observable[ConfidentialContractDataUpdate],
    time: Time
)(implicit scheduler: Scheduler)
    extends ConfidentialDataInventoryBroadcaster
    with ScorexLogging
    with ConfidentialDataSynchronizerMetrics
    with AutoCloseable {

  private type FibersMap = collection.concurrent.Map[ConfidentialDataId, Fiber[Unit]]

  private[this] val synchronization = SerialCancelable()

  import ConfidentialDataSynchronizer._
  import ConfidentialDataSynchronizerError._

  private[this] val synchronizedOutputsSubject            = ConcurrentSubject.publish[ConfidentialOutput]
  def synchronizedOutputs: Observable[ConfidentialOutput] = synchronizedOutputsSubject

  def run(): Unit = {
    log.debug("Run confidential contract data synchronizer")
    internalRun()
  }

  private[network] def internalRun(): FibersMap =
    runSynchronization(storage.pendingDataItems())

  def forceSync(): Int = {
    log.debug("Attempting force sync")
    val (_, restartedCount) = internalForceSync()
    restartedCount
  }

  private[network] def internalForceSync(): (FibersMap, Int) = {
    val pending = storage.pendingDataItems()
    runSynchronization(pending) -> pending.size
  }

  def forceSync(contractId: ContractId): Int = {
    log.debug(s"Attempting force sync for contractId: '$contractId'")
    val (_, restartedCount) = internalForceSync(contractId)
    restartedCount
  }

  private[network] def internalForceSync(contractId: ContractId): (FibersMap, Int) = {
    val pending = storage.pendingDataItems()
    val fromBlockChain = storage.contractDataItems(contractId)
      .map(contractDataId => ConfidentialDataId(contractId, contractDataId.commitment, contractDataId.dataType))

    val maybeMissedTasks = fromBlockChain -- pending
    if (maybeMissedTasks.nonEmpty) {
      val added = storage.addToPending(maybeMissedTasks)
      additionsToPending.increment(added)
      log.trace(s"Recovered missed requests: ${maybeMissedTasks.mkString("[", ", ", "]")}")
    }
    val pendingIds = pending ++ fromBlockChain
    runSynchronization(pendingIds) -> pendingIds.size
  }

  override def close(): Unit = synchronization.cancel()

  private def runSynchronization(initPendingIds: Set[ConfidentialDataId]): FibersMap = {
    log.info(s"Start confidential data synchronizer with '${initPendingIds.size}' pending items")

    val crawlingFibers = TrieMap.empty[ConfidentialDataId, Fiber[Unit]]

    val confidentialDataUpdates = Observable(blockchain.confidentialDataUpdates, confidentialDataUpdatesFromUtx).merge
    val crawlingStream          = buildCrawlingStream(confidentialDataUpdates, crawlingFibers, initPendingIds)
    val rollbackStream          = buildRollbackStream(blockchain.confidentialDataRollbacks, crawlingFibers)

    synchronization := Observable(crawlingStream, rollbackStream).merge
      .guarantee {
        Task {
          val pendingSize = storage.pendingDataItems().size
          log.info(s"Confidential contract data synchronizer stops, pending tasks count '$pendingSize'")
        } >> crawlingFibers.values.map(_.cancel).toList.parSequence_
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

    crawlingFibers
  }

  private def buildCrawlingStream(confidentialDataUpdates: Observable[ConfidentialContractDataUpdate],
                                  crawlingFibers: FibersMap,
                                  pendingIds: Iterable[ConfidentialDataId]): Observable[Unit] = {
    Observable
      .fromTask(Semaphore[Task](settings.crawlingParallelism.value))
      .flatMap { crawlingSemaphore =>
        val lostDataSubStream      = buildLostDataSubStream(crawlingSemaphore, crawlingFibers)
        val pendingDataSubStream   = buildPendingDataSubStream(pendingIds, crawlingSemaphore, crawlingFibers)
        val policyUpdatesSubStream = buildPolicyUpdatesSubStream(confidentialDataUpdates, crawlingSemaphore, crawlingFibers)

        Observable(lostDataSubStream, pendingDataSubStream, policyUpdatesSubStream).merge
      }
  }

  private def buildRollbackStream(confidentialDataRollbacks: Observable[ConfidentialDataId], crawlingFibers: FibersMap): Observable[Unit] = {
    confidentialDataRollbacks
      .asyncBoundary(OverflowStrategy.Default)
      .mapEval { dataId =>
        Task(log.debug(s"Rollback for $dataId")) *>
          crawlingFibers.get(dataId).fold(Task.unit)(_.cancel)
      }
  }

  private def buildLostDataSubStream(crawlingSemaphore: Semaphore[Task], crawlingFibers: FibersMap): Observable[Unit] = {
    val lostProcessingParallelism = math.max(1, settings.crawlingParallelism.value / 4)

    def forever(lostProcessingSemaphore: Semaphore[Task]): Observable[Unit] =
      Observable(
        Observable
          .fromIterable {
            val lostItems = Random.shuffle(storage.lostDataItems().toSeq)
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

  private def buildPendingDataSubStream(pendingIds: Iterable[ConfidentialDataId],
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

  private def buildPolicyUpdatesSubStream(policyUpdates: Observable[ConfidentialContractDataUpdate],
                                          crawlingSemaphore: Semaphore[Task],
                                          crawlingFibers: FibersMap): Observable[Unit] = {
    policyUpdates
      .asyncBoundary(OverflowStrategy.Default)
      .mapEval {
        case ConfidentialContractDataUpdate(key, maybeTxTs) =>
          crawl(crawlingSemaphore, crawlingFibers, key, maybeTxTs)
      }
  }

  private def crawl(crawlingSemaphore: Semaphore[Task],
                    crawlingFibers: FibersMap,
                    key: ConfidentialDataId,
                    maybeTxTimestamp: Option[Long] = None,
                    externalGuarantee: Task[Unit] = Task.unit): Task[Unit] = {

    def removeFiber(): Task[Unit] = Task {
      log.trace(s"Remove crawling fiber for $key")
      crawlingFibers.remove(key)
    }

    def addFiber(fiber: Fiber[Unit]): Task[Unit] = Task {
      log.trace(s"Create new crawling fiber for $key")
      crawlingFibers.put(key, fiber)
    }

    crawlingSemaphore.acquire *> ensureDataInStorage(key, maybeTxTimestamp)
      .guarantee {
        (crawlingSemaphore.release, externalGuarantee, removeFiber()).parTupled.void
      }
      .start
      .flatMap(addFiber)
  }

  private def ensureDataInStorage(dataId: ConfidentialDataId, maybeTxTimestamp: Option[Long]): Task[Unit] =
    Task
      .defer {
        val exists = storage.dataExists(dataId.commitment, dataId.dataType)
        (if (exists) {
           Task(log.debug(s"Data for $dataId already in storage"))
         } else {
           for {
             _            <- Task(log.debug(s"Start data crawling for $dataId"))
             startedTimer <- Task(loadingTaskExecution.start())
             _            <- pullFromPeers(dataId, settings.initRetryDelay)
             _            <- Task(startedTimer.stop())
             _            <- buildAndBroadcastInventoryIfNeeded(dataId, maybeTxTimestamp)
           } yield ()
         }) *> Task {
          val (removedFromPending, removedFromLost) = storage.removeFromPendingAndLost(dataId)
          log.debug(s"Completed data crawling for $dataId")
          (removedFromPending, removedFromLost)
        } flatMap { case (removedFromPending, removedFromLost) =>
          Task {
            if (removedFromPending) removalsFromPending.increment()
            if (removedFromLost) removalsFromLost.increment()
          }
        }
      }
      .onErrorHandleWith { ex =>
        Task(log.warn(s"Failed to ensure the availability of $dataId", ex))
      }

  private def buildAndBroadcastInventoryIfNeeded(dataId: ConfidentialDataId, maybeTxTimestamp: Option[Long]): Task[Unit] =
    Task.defer {
      if (maybeTxTimestamp.exists(txTs => time.correctedTime() - txTs < settings.inventoryTimestampThreshold.toMillis)) {
        buildConfidentialInventory(dataId.contractId, dataId.commitment, dataId.dataType, owner)
          .map { inventory =>
            broadcastInventory(inventory)
            log.debug(s"Inventory for $dataId has been sent")
          }
      } else {
        Task {
          log.debug(
            s"Inventory for $dataId is not generated because " +
              maybeTxTimestamp.fold("target timestamp is undefined")(txTs => s"target timestamp '$txTs' is too old")
          )
        }
      }
    }

  private def pullFromPeers(dataId: ConfidentialDataId,
                            retryDelay: FiniteDuration,
                            attemptsCount: Int = 1,
                            withInventoryRequest: Boolean = false): Task[Unit] =
    Task.defer {
      blockchain.contract(dataId.contractId) match {
        case Some(contract) =>
          val recipients    = contract.groupParticipants
          val selectedPeers = selectPeers(recipients, dataId.contractId)
          if (selectedPeers.nonEmpty) {
            val maybeInventoryRequest =
              if (withInventoryRequest || !confidentialDataInventoryHandler.containsInventoryDataOwners(dataId)) {
                sendToPeers(ConfidentialInventoryRequest(dataId.contractId, dataId.commitment, dataId.dataType), selectedPeers: _*) *>
                  Task(inventoryRequests.increment()) *>
                  Task {
                    log.debug(s"Inventory request for $dataId")
                  }.delayResult(settings.inventoryRequestDelay)
              } else {
                Task(log.trace(s"No need to request inventory for $dataId"))
              }

            maybeInventoryRequest *> pullByInventory(recipients, dataId, attemptsCount, retryDelay)
          } else {
            pullFallback(dataId, attemptsCount, retryDelay)
          }
        case None =>
          Task.raiseError(CustomError(s"Confidential data for $dataId will not be pulled, because the contract not found in DB"))
      }

    }

  protected def selectPeers(groupParticipants: Set[Address], contractId: ContractId): Vector[PeerSession] = {
    val groupParticipantPeerSessions = peers.withAddresses(groupParticipants.contains, excludeWatchers = true).toVector

    if (groupParticipantPeerSessions.isEmpty) {
      log.warn(s"Couldn't find participants of confidential contract '$contractId' among connected peers, scheduling retry")
    }

    groupParticipantPeerSessions
  }

  private def pullFallback(dataId: ConfidentialDataId,
                           pullAttemptsCount: Int,
                           retryDelay: FiniteDuration,
                           withInventoryRequest: Boolean = false): Task[Unit] =
    Task.defer {
      if (pullAttemptsCount + 1 > settings.maxAttemptCount.value) {
        Task.defer {
          storage.pendingToLost(dataId)
          val error = LostDataException(dataId)
          log.warn(error.message)
          Task.raiseError(error)
        } *>
          Task {
            removalsFromPending.increment()
            additionsToLost.increment()
          }
      } else {
        val nextPullAttempt = pullFromPeers(
          dataId,
          retryDelay = (retryDelay.toMillis * PullRetryDelayFactor).millis,
          attemptsCount = pullAttemptsCount + 1,
          withInventoryRequest
        )

        nextPullAttempt.delayExecution(retryDelay)
      }
    }

  import scala.collection.JavaConverters._

  private val failedAddresses =
    CacheBuilder
      .newBuilder()
      .maximumSize(settings.failedPeersCacheSize)
      .expireAfterWrite(settings.failedPeersCacheExpireTimeout.toMillis, TimeUnit.MILLISECONDS)
      .build[Address, Address]()
      .asMap()
      .asScala

  private def pullByInventory(groupParticipants: Set[Address],
                              dataId: ConfidentialDataId,
                              pullAttemptsCount: Int,
                              retryDelay: FiniteDuration): Task[Unit] = {
    val coarsestRetryDelay = retryDelay.toSeconds.seconds.toCoarsest

    confidentialDataInventoryHandler
      .inventoryObservable(dataId)
      .flatMap { inventoryOwners =>
        val filteredInventorySenders = inventoryOwners.filterNot(failedAddresses.contains)
        val descriptorsToProcess = if (filteredInventorySenders.nonEmpty) {
          filteredInventorySenders
        } else {
          failedAddresses.clear()
          inventoryOwners
        }
        val shuffledInventorySenders = Random.shuffle(descriptorsToProcess.toSeq)
        log.trace(s"Going to process inventories from following senders: [$shuffledInventorySenders]")
        Observable.fromIterable(shuffledInventorySenders)
      }
      .asyncBoundary(OverflowStrategy.Default)
      .timeoutOnSlowUpstream(settings.inventoryStreamTimeout)
      .flatMap { inventorySender =>
        log.trace(s"Process $inventorySender")
        val validatedInventoryAddress = groupParticipants.find(_ == inventorySender)
        Observable.fromIterable {
          peers
            .withAddresses(validatedInventoryAddress.contains, excludeWatchers = true)
        }
      }
      .mapEval { session =>
        requestAndProcessData(dataId, session)
          .onErrorRecover {
            case NonFatal(error) =>
              log.warn(
                s"Failed to retrieve response for $dataId with address '${session.peerInfo.nodeOwnerAddress}'" +
                  s" and channel '${id(session.channel)}'",
                error
              )
          }
      }
      .firstL
      .onErrorRecoverWith {
        case _: UpstreamTimeoutException =>
          Task(log.warn(s"Slow inventory upstream for $dataId, retrying after $coarsestRetryDelay")) *>
            pullFallback(dataId, pullAttemptsCount, retryDelay, withInventoryRequest = true) *>
            Task(inventoryIterations.increment())
        case NonFatal(ex) =>
          Task {
            log.error(
              s"Failed to pull data by inventory for $dataId, retrying after $coarsestRetryDelay",
              ex
            )
          } *> pullFallback(dataId, pullAttemptsCount, retryDelay)
      }
  }

  private def requestAndProcessData(dataId: ConfidentialDataId, peer: PeerSession): Task[Unit] = {
    def startAwaitingAndBuildProcessingTask(dataRequest: ConfidentialDataRequest): Task[Task[Unit]] = {
      awaitStrictResponse(dataId, peer.channel)
        .map(processStrictResponse(dataRequest, dataId, peer.channel, _))
    }

    val dataRequest = ConfidentialDataRequest(dataId.contractId, dataId.commitment, dataId.dataType)

    (for {
      processingTask <- startAwaitingAndBuildProcessingTask(dataRequest)
      _              <- sendToPeers(dataRequest, peer)
      _ <- processingTask.onError {
        case _: UpstreamTimeoutException => Task(failedAddresses.put(peer.address, peer.address)).void
      }
    } yield ()).timeout(settings.requestTimeout)
  }

  private def awaitStrictResponse(dataId: ConfidentialDataId, channel: Channel): Task[Fiber[HasDataResponse]] = {
    responses
      .asyncBoundary(OverflowStrategy.Default)
      .filter { case (ch, response) =>
        ch == channel && dataId.contractId == response.contractId && response.commitment == response.commitment && dataId.dataType == response.dataType
      }
      .mapEval {
        case (_, response: ConfidentialDataResponse.HasDataResponse) =>
          Task.pure(response)
        case _ =>
          log.debug(s"Peer '${id(channel)}' have no data for $dataId")
          Task.raiseError(DataNotFoundError(dataId))
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

  private def processStrictResponse(dataRequest: ConfidentialDataRequest,
                                    dataId: ConfidentialDataId,
                                    channel: Channel,
                                    responseAwaiting: Fiber[HasDataResponse]): Task[Unit] = {

    responseAwaiting.join
      .flatMap { response =>
        val dataEncrypted = !channel.hasAttr(TlsAttribute)

        response match {
          case responseWithData: HasDataResponse if dataEncrypted =>
            for {
              startedTimer <- Task(decryptingConfidentialData.start())
              dataEither <- Task.fromEither(ConfidentialDataSynchronizer
                .decryptResponse(dataId, responseWithData.data, peers, channel))
              _ <- Task(startedTimer.stop())
            } yield responseWithData -> dataEither

          case responseWithData: HasDataResponse =>
            Task.pure(responseWithData -> responseWithData.data)
        }
      }
      .flatMap { case (response, dataBytes) =>
        log.debug(s"Saving confidential data for $dataId")

        val saveTask = response.dataType match {
          case ConfidentialDataType.Input =>
            val input = ConfidentialInput(
              response.commitment,
              response.txId,
              response.contractId,
              response.commitmentKey,
              ConfidentialDataUtils.fromBytes(dataBytes.arr)
            )
            Task(storage.saveInput(input))
          case ConfidentialDataType.Output =>
            val output = ConfidentialOutput(
              response.commitment,
              response.txId,
              response.contractId,
              response.commitmentKey,
              ConfidentialDataUtils.fromBytes(dataBytes.arr)
            )

            Task(storage.saveOutput(output)) *>
              Task.fromFuture(synchronizedOutputsSubject.onNext(output))
        }
        saveTask *> Task(dataCache.invalidate(dataRequest))
      }

  }
}

object ConfidentialDataSynchronizer extends ScorexLogging {

  val PullRetryDelayFactor: Double = 4.0 / 3

  def decryptResponse(dataId: ConfidentialDataId,
                      encryptedByteStr: ByteStr,
                      peers: ActivePeerConnections,
                      channel: Channel): Either[ConfidentialDataSynchronizerError, ByteStr] = {

    def parseEncryptedStructure(encryptedBytes: Array[Byte]): EncryptedForSingle = {
      val encryptedBuf = ByteBuffer.wrap(encryptedBytes)

      val wrappedKey = new Array[Byte](com.wavesenterprise.crypto.WrappedStructureLength)
      encryptedBuf.get(wrappedKey)

      val encryptedDataLength = new Array[Byte](Ints.BYTES)
      encryptedBuf.get(encryptedDataLength)

      val encryptedData = new Array[Byte](Ints.fromByteArray(encryptedDataLength))
      encryptedBuf.get(encryptedData)

      EncryptedForSingle(encryptedData, wrappedKey)
    }

    for {
      peerConnection <- peers.peerConnection(channel).toRight(NoPeerInfo(channel))
      peerPublicKey = peerConnection.peerInfo.sessionPubKey.publicKey
      encrypted     = parseEncryptedStructure(encryptedByteStr.arr)
      decryptedBytes <- crypto
        .decrypt(encrypted, peerConnection.sessionKey.privateKey, peerPublicKey)
        .leftMap(cause => DecryptionError(dataId, cause))
    } yield ByteStr(decryptedBytes)

  }

}
