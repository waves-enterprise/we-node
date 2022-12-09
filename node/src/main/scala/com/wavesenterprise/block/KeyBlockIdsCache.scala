package com.wavesenterprise.block

import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesenterprise.settings.BlockIdsCacheSettings
import com.wavesenterprise.state.ByteStr

import java.util.concurrent.TimeUnit

class KeyBlockIdsCache(settings: BlockIdsCacheSettings) {

  private[this] var maybeCurrentKeyBlock: Option[ByteStr] = None

  private[this] val cache: Cache[ByteStr, ByteStr] =
    CacheBuilder
      .newBuilder()
      .maximumSize(settings.maxSize)
      .expireAfterWrite(settings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
      .build()

  def containsWithoutCurrent(id: ByteStr): Boolean = cache.asMap().containsKey(id)

  def put(id: ByteStr): Unit = synchronized {
    val maybeOldCurrent = maybeCurrentKeyBlock
    maybeCurrentKeyBlock = Some(id)

    maybeOldCurrent.foreach { oldCurrent =>
      cache.put(oldCurrent, oldCurrent)
    }
  }
}
