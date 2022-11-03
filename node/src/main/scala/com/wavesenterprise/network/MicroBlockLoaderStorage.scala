package com.wavesenterprise.network

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.Sets
import com.wavesenterprise.block.MicroBlock
import com.wavesenterprise.metrics.BlockStats
import com.wavesenterprise.network.MicroBlockLoader.{MicroBlockSignature, ReceivedMicroBlock}
import com.wavesenterprise.network.MicroBlockLoaderStorage.CacheSizes
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.settings.SynchronizationSettings.MicroblockSynchronizerSettings
import com.wavesenterprise.state.ByteStr
import io.netty.channel.Channel
import monix.eval.Coeval

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable.{Set => MutableSet}
import scala.concurrent.duration.FiniteDuration

class MicroBlockLoaderStorage(settings: MicroblockSynchronizerSettings) {

  private def cache[K <: AnyRef, V <: AnyRef](timeout: FiniteDuration): Cache[K, V] =
    CacheBuilder
      .newBuilder()
      .expireAfterWrite(timeout.toMillis, TimeUnit.MILLISECONDS)
      .maximumSize(settings.maxCacheSize)
      .build[K, V]()

  private[this] val microBlocksByPrevSign         = cache[MicroBlockSignature, ReceivedMicroBlock](settings.processedMicroBlocksCacheTimeout)
  private[this] val microBlocksByTotalSign        = cache[MicroBlockSignature, MicroBlock](settings.processedMicroBlocksCacheTimeout)
  private[this] val inventories                   = cache[MicroBlockSignature, MicroBlockInventory](settings.inventoryCacheTimeout)
  private[this] val owners                        = cache[MicroBlockSignature, MutableSet[Channel]](settings.inventoryCacheTimeout)
  private[this] val certChainStoreByMicroBlockSig = cache[MicroBlockSignature, CertChainStore](settings.processedMicroBlocksCacheTimeout) // TODO: validate everything
  private[this] val crlHashesByMicroBlockSig      = cache[MicroBlockSignature, Set[ByteStr]](settings.processedMicroBlocksCacheTimeout)

  val cacheSizesReporter: Coeval[CacheSizes] = Coeval {
    CacheSizes(owners.size(), inventories.size(), microBlocksByTotalSign.size)
  }

  def findMicroBlockByPrevSign(reference: MicroBlockSignature): Option[ReceivedMicroBlock] =
    Option(microBlocksByPrevSign.getIfPresent(reference))

  def findMicroBlockByTotalSign(sign: MicroBlockSignature): Option[MicroBlock] =
    Option(microBlocksByTotalSign.getIfPresent(sign))

  def findCertChainStoreByTotalSign(sign: MicroBlockSignature): Option[CertChainStore] =
    Option(certChainStoreByMicroBlockSig.getIfPresent(sign))

  def findCrlHashesByTotalSign(sign: MicroBlockSignature): Set[ByteStr] =
    Option(crlHashesByMicroBlockSig.getIfPresent(sign)).toSet.flatten

  def isKnownMicroBlock(totalSignature: MicroBlockSignature): Boolean =
    Option(owners.getIfPresent(totalSignature)).exists(_.nonEmpty)

  def currentOwners(microBlockId: MicroBlockSignature): Set[Channel] =
    Option(owners.getIfPresent(microBlockId))
      .getOrElse(MutableSet.empty)
      .toSet

  def addOwner(channel: Channel, totalSign: MicroBlockSignature): Boolean = {
    val ownerChannels = owners.get(totalSign, () => Sets.newConcurrentHashSet[Channel]().asScala)
    ownerChannels.add(channel)
  }

  def isUnknownInventory(inventory: MicroBlockInventory, currentChannel: Channel): Boolean = {
    var isUnknown = false
    inventories.get(inventory.prevBlockSig, { () =>
      BlockStats.inventory(inventory, currentChannel)
      isUnknown = true
      inventory
    })
    isUnknown
  }

  def put(entry: ReceivedMicroBlock): Unit = {
    microBlocksByPrevSign.put(entry.microBlock.prevLiquidBlockSig, entry)
    microBlocksByTotalSign.put(entry.microBlock.totalLiquidBlockSig, entry.microBlock)
    if (CertChainStore.empty != entry.certChainStore) {
      certChainStoreByMicroBlockSig.put(entry.microBlock.totalLiquidBlockSig, entry.certChainStore)
    }
  }
}

object MicroBlockLoaderStorage {
  case class CacheSizes(microBlockOwners: Long, inventories: Long, successfullyReceived: Long)
}
