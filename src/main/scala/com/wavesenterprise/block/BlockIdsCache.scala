package com.wavesenterprise.block

import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesenterprise.settings.BlockIdsCacheSettings
import com.wavesenterprise.state.ByteStr

case class BlockIdsCache(settings: BlockIdsCacheSettings) {
  private val cache: Cache[ByteStr, ByteStr] =
    CacheBuilder
      .newBuilder()
      .maximumSize(settings.maxSize)
      .expireAfterAccess(settings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
      .build()

  def contains(id: ByteStr): Boolean = cache.asMap().containsKey(id)
  def put(id: ByteStr): Unit         = cache.put(id, id)
  def invalidate(id: ByteStr): Unit  = cache.invalidate(id)
}
