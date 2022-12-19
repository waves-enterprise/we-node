package com.wavesenterprise.network.privacy

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalNotification}
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.privacy.PrivacyInventoryHandler.InventoryDescriptor
import com.wavesenterprise.network.{ChannelObservable, PrivacyInventory, PrivacyInventoryRequest, id}
import com.wavesenterprise.privacy.{PolicyDataId, PolicyStorage, PrivacyDataType}
import com.wavesenterprise.settings.privacy.PrivacyInventoryHandlerSettings
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import monix.eval.Task
import monix.execution.cancelables.SerialCancelable
import monix.execution.{Cancelable, Scheduler}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

class PrivacyInventoryHandler(
    inventories: ChannelObservable[PrivacyInventory],
    requests: ChannelObservable[PrivacyInventoryRequest],
    protected val owner: PrivateKeyAccount,
    protected val peers: ActivePeerConnections,
    protected val state: Blockchain with PrivacyState,
    protected val storage: PolicyStorage,
    settings: PrivacyInventoryHandlerSettings
)(implicit val scheduler: Scheduler)
    extends ScorexLogging
    with PolicyInventoryBroadcaster
    with PolicyItemTypeSelector
    with AutoCloseable {

  private val requestsProcessing    = SerialCancelable()
  private val inventoriesProcessing = SerialCancelable()

  override def close(): Unit = {
    requestsProcessing.cancel()
    inventoriesProcessing.cancel()
  }

  def run(): Unit = {
    requestsProcessing    := runRequestProcessing()
    inventoriesProcessing := runInventoryProcessing()
  }

  def containsInventoryDataOwners(policyDataId: PolicyDataId): Boolean =
    dataOwnersCache.asMap.containsKey(policyDataId)

  private def publishInventoryDescriptor(policyDataId: PolicyDataId, inventoryDescriptor: InventoryDescriptor): Unit = {
    val descriptors = dataOwnersCache.get(policyDataId)
    descriptors.add(inventoryDescriptor)
    inventorySubject(policyDataId).onNext(descriptors.toSet)
  }

  def inventoryObservable(policyDataId: PolicyDataId): Observable[Set[InventoryDescriptor]] = {
    Observable
      .eval {
        dataOwnersCache.get(policyDataId)
      }
      .map(_.toSet) ++ inventorySubject(policyDataId)
  }

  @inline
  private def inventorySubject(policyDataId: PolicyDataId) = {
    dataOwnersObservables.get(policyDataId, () => ConcurrentSubject.publish[Set[InventoryDescriptor]])
  }

  private def runRequestProcessing(): Cancelable =
    requests
      .asyncBoundary(OverflowStrategy.Default)
      .mapParallelUnordered(settings.replierParallelism.value) {
        case (channel, request) =>
          val policyRecipients = state.policyRecipients(request.policyId)
          if (peers.addressForChannel(channel).exists(policyRecipients.contains)) {
            storage.policyItemExists(request.policyId.toString, request.dataHash.toString).flatMap {
              case Left(error) =>
                Task(log.error(s"Failed to check policy item existing. Policy '${request.policyId}' data hash '${request.dataHash}', error: $error"))
              case Right(true) =>
                for {
                  dataType  <- policyItemType(request.policyId, request.dataHash)
                  inventory <- buildPrivacyInventory(dataType, request.policyId, request.dataHash, owner)
                  _ = channel.writeAndFlush(inventory)
                } yield ()
              case Right(false) =>
                Task(log.debug(s"No data found for policy '${request.policyId}' and data '${request.dataHash}'"))
            }
          } else {
            Task(log.warn(s"Channel '${id(channel)}' address not included in the policy '${request.policyId}' recipients"))
          }
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

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
      .removalListener { (notification: RemovalNotification[PolicyDataId, ConcurrentSubject[Set[InventoryDescriptor], Set[InventoryDescriptor]]]) =>
        notification.getValue.onComplete()
      }
      .build[PolicyDataId, ConcurrentSubject[Set[InventoryDescriptor], Set[InventoryDescriptor]]]

  private[this] val dataOwnersCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(settings.maxCacheSize.value)
      .expireAfterWrite(settings.expirationTime.toMillis, TimeUnit.MILLISECONDS)
      .build[PolicyDataId, mutable.Set[InventoryDescriptor]](new CacheLoader[PolicyDataId, mutable.Set[InventoryDescriptor]] {
        override def load(key: PolicyDataId): mutable.Set[InventoryDescriptor] = {
          import scala.collection.JavaConverters._
          java.util.concurrent.ConcurrentHashMap.newKeySet[InventoryDescriptor]().asScala
        }
      })

  private def runInventoryProcessing() =
    inventories
      .asyncBoundary(OverflowStrategy.Default)
      .bufferTimedAndCounted(settings.maxBufferTime, settings.maxBufferSize.value)
      .map { messagesBuffer =>
        val toAdd = messagesBuffer.filter {
          case (_, inventory) =>
            isUnknown(inventory) && inventory.signatureValid()
        }

        if (toAdd.nonEmpty) {
          toAdd
            .groupBy { case (channel, inventory) => channel -> inventory.policyId }
            .foreach {
              case ((channel, policyId), inventoriesGroup) =>
                handleChannelInventories(channel, policyId, inventoriesGroup)
            }
          peers.flushWrites()
        }
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

  private def handleChannelInventories(sender: Channel, policyId: ByteStr, inventories: Seq[(Channel, PrivacyInventory)]): Unit = {
    val policyRecipients = state.policyRecipients(policyId)
    inventories.foreach {
      case (_, inventory) =>
        publishInventoryDescriptor(inventory.dataId, InventoryDescriptor(inventory.sender.toAddress, inventory.dataType))
        log.trace(s"Broadcast $inventory")
        broadcastInventoryToRecipients(policyRecipients, inventory, flushChannels = false, excludeChannels = Set(sender))
    }
  }

  private[this] val dummy = new Object()

  @inline
  private def isUnknown(inventory: PrivacyInventory): Boolean = {
    var isUnknown = false
    knownInventories.get(inventory.id(),
                         { () =>
                           isUnknown = true
                           dummy
                         })
    isUnknown
  }
}

object PrivacyInventoryHandler {
  case class InventoryDescriptor(senderAddress: Address, dataType: PrivacyDataType)
}
