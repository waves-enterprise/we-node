package com.wavesenterprise.network.contracts

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalNotification}
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{ChannelObservable, ConfidentialInventory, ConfidentialInventoryRequest, id}
import com.wavesenterprise.settings.contract.ConfidentialInventoryHandlerSettings
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import monix.eval.Task
import monix.execution.cancelables.SerialCancelable
import monix.execution.{Cancelable, Scheduler}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

class ConfidentialDataInventoryHandler(
    inventories: ChannelObservable[ConfidentialInventory],
    requests: ChannelObservable[ConfidentialInventoryRequest],
    protected val owner: PrivateKeyAccount,
    protected val peers: ActivePeerConnections,
    protected val blockchain: Blockchain,
    protected val storage: ConfidentialRocksDBStorage,
    settings: ConfidentialInventoryHandlerSettings
)(implicit val scheduler: Scheduler)
    extends ScorexLogging
    with ConfidentialDataInventoryBroadcaster
    with ConfidentialDataInventoryMetrics
    with AutoCloseable {

  private val requestsProcessing    = SerialCancelable()
  private val inventoriesProcessing = SerialCancelable()

  private[this] val knownInventories =
    CacheBuilder
      .newBuilder()
      .maximumSize(settings.maxCacheSize.value)
      .expireAfterWrite(settings.expirationTime.toMillis, TimeUnit.MILLISECONDS)
      .build[ByteStr, Object]

  private[this] val dataOwnersObservables =
    CacheBuilder
      .newBuilder()
      .maximumSize(settings.maxCacheSize.value)
      .expireAfterWrite(settings.expirationTime.toMillis, TimeUnit.MILLISECONDS)
      .removalListener {
        (notification: RemovalNotification[ConfidentialDataId, ConcurrentSubject[Set[Address], Set[Address]]]) =>
          notification.getValue.onComplete()
      }
      .build[ConfidentialDataId, ConcurrentSubject[Set[Address], Set[Address]]]

  private[this] val dataOwnersCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(settings.maxCacheSize.value)
      .expireAfterWrite(settings.expirationTime.toMillis, TimeUnit.MILLISECONDS)
      .build[ConfidentialDataId, mutable.Set[Address]](new CacheLoader[ConfidentialDataId, mutable.Set[Address]] {
        override def load(key: ConfidentialDataId): mutable.Set[Address] = {
          import scala.collection.JavaConverters._
          java.util.concurrent.ConcurrentHashMap.newKeySet[Address]().asScala
        }
      })

  override def close(): Unit = {
    requestsProcessing.cancel()
    inventoriesProcessing.cancel()
  }

  def run(): Unit = {
    requestsProcessing    := runRequestProcessing()
    inventoriesProcessing := runInventoryProcessing()
  }

  def containsInventoryDataOwners(confidentialDataId: ConfidentialDataId): Boolean =
    dataOwnersCache.asMap.containsKey(confidentialDataId)

  private def publishInventoryDescriptor(confidentialDataId: ConfidentialDataId, senderAddress: Address): Unit = {
    val descriptors = dataOwnersCache.get(confidentialDataId)
    descriptors.add(senderAddress)
    inventorySubject(confidentialDataId).onNext(descriptors.toSet) // wtf is going on, why not smth like atmc reference or monix Var
  }

  def inventoryObservable(confidentialDataId: ConfidentialDataId): Observable[Set[Address]] = {
    Observable
      .eval {
        dataOwnersCache.get(confidentialDataId)
      }
      .map(_.toSet) ++ inventorySubject(confidentialDataId)
  }

  @inline
  private def inventorySubject(confidentialDataId: ConfidentialDataId) = {
    dataOwnersObservables.get(confidentialDataId, () => ConcurrentSubject.publish[Set[Address]])
  }

  private def runRequestProcessing(): Cancelable =
    requests
      .asyncBoundary(OverflowStrategy.Default)
      .mapParallelUnordered(settings.replierParallelism.value) {
        case (channel, request) => for {
            startedTimer <- Task(inventoryRequestProcessing.start())
            _ <- blockchain.contract(request.contractId) match {
              case Some(contract) =>
                val confidentialDataRecipients = contract.groupParticipants
                if (peers.addressForChannel(channel).exists(confidentialDataRecipients.contains)) {
                  val isDataExists = request.dataType match {
                    case ConfidentialDataType.Input  => storage.getInput(request.commitment).isDefined
                    case ConfidentialDataType.Output => storage.getOutput(request.commitment).isDefined
                  }

                  if (isDataExists) {
                    for {
                      inventory <- buildConfidentialInventory(request.contractId, request.commitment, request.dataType, owner) *> Task(
                        inventoryRequests.refine("dataPresence" -> "yes"))
                      _ = channel.writeAndFlush(inventory)
                    } yield ()
                  } else {
                    Task(log.error(s"No data found for $request")) *> Task(inventoryRequests.refine("dataPresence" -> "no"))
                  }
                } else {
                  Task(log.warn(s"Channel '${id(channel)}' address not included in the " +
                    s"confidential data of contract '${request.contractId}' recipients"))
                }
              case None =>
                Task(log.error(s"$request will not be precessed, because contract '${request.contractId}' not found in DB"))
            }
            _ = Task(startedTimer.stop())
          } yield ()
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

  private def runInventoryProcessing() =
    inventories
      .asyncBoundary(OverflowStrategy.Default)
      .bufferTimedAndCounted(settings.maxBufferTime, settings.maxBufferSize.value)
      .map { messagesBuffer =>

        incomingInventories.increment(messagesBuffer.length)

        val toAdd = messagesBuffer.filter {
          case (_, inventory) =>
            isUnknown(inventory) && inventory.signatureValid()
        }

        if (toAdd.nonEmpty) {
          toAdd
            .groupBy { case (channel, inventory) => channel -> inventory.contractId }
            .foreach {
              case ((channel, contractId), inventoriesGroup) =>
                handleChannelInventories(channel, contractId, inventoriesGroup)
            }
          peers.flushWrites()
        }
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

  private def handleChannelInventories(sender: Channel, contractId: ContractId, inventories: Seq[(Channel, ConfidentialInventory)]): Unit = {
    val maybeContract = blockchain.contract(contractId)

    maybeContract match {
      case Some(contract) =>
        inventories.foreach {
          case (_, inventory) =>
            publishInventoryDescriptor(inventory.dataId, inventory.sender.toAddress)
            log.trace(s"Broadcast $inventory")
            broadcastInventoryToRecipients(contract.groupParticipants, inventory, flushChannels = false, excludeChannels = Set(sender))
        }
        filteredAndProcessedInventories.increment(inventories.length)
      case None =>
        Task(log.error(s"Inventories for contract $contractId will not be processed, because the contract not found in DB"))
    }

  }

  private[this] val dummy = new Object()

  @inline
  private def isUnknown(inventory: ConfidentialInventory): Boolean = {
    var isUnknown = false
    knownInventories.get(inventory.id(),
                         { () =>
                           isUnknown = true
                           dummy
                         })
    isUnknown
  }
}
