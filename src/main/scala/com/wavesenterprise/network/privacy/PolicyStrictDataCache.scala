package com.wavesenterprise.network.privacy

import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesenterprise.network.privacy.PolicyDataReplier.StrictPolicyData
import com.wavesenterprise.privacy.PolicyDataId
import com.wavesenterprise.settings.privacy.PolicyDataCacheSettings
import monix.eval.Task

import java.util.concurrent.TimeUnit

class PolicyStrictDataCache(settings: PolicyDataCacheSettings) {

  private[this] val cache: Cache[PolicyDataId, Task[StrictPolicyData]] = CacheBuilder
    .newBuilder()
    .maximumSize(settings.maxSize)
    .expireAfterAccess(settings.expireAfter.toMillis, TimeUnit.MILLISECONDS)
    .build()

  def getOrLoad(policyDataId: PolicyDataId)(task: Task[StrictPolicyData]): Task[StrictPolicyData] =
    cache.get(policyDataId, () => task.memoizeOnSuccess)

  def invalidate(policyDataId: PolicyDataId): Unit =
    cache.invalidate(policyDataId)
}
