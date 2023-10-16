package com.wavesenterprise.utils

import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesenterprise.settings.LRUCacheSettings
import monix.eval.Task

import java.util.concurrent.TimeUnit

class AsyncLRUCache[K <: AnyRef, V](settings: LRUCacheSettings) {

  private[this] val cache: Cache[K, Task[V]] = CacheBuilder
    .newBuilder()
    .maximumSize(settings.maxSize)
    .expireAfterAccess(settings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
    .build[K, Task[V]]()

  def getOrLoad(policyDataId: K)(task: Task[V]): Task[V] =
    cache.get(policyDataId, () => task.memoizeOnSuccess)

  def invalidate(policyDataId: K): Unit =
    cache.invalidate(policyDataId)
}
