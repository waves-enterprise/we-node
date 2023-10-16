package com.wavesenterprise.database.migration

import com.google.common.primitives.Shorts
import com.wavesenterprise.database.{InternalRocksDBSet, MainDBKey}
import com.wavesenterprise.database.Keys.AssetIdsPrefix
import com.wavesenterprise.database.keys.PrivacyCFKeys.PolicyIdsPrefix
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.{PresetCF, PrivacyCF}
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ResourceUtils.withResource

/**
  * Additional migration to fix broken [[MigrationV3]] consequences
  */
object MigrationV6 {

  private val PolicyIdsSet = new InternalRocksDBSet[ByteStr, MainDBColumnFamily](
    name = "policy-ids",
    columnFamily = PrivacyCF,
    prefix = Shorts.toByteArray(PolicyIdsPrefix),
    itemEncoder = (_: ByteStr).arr,
    itemDecoder = ByteStr(_),
    keyConstructors = MainDBKey
  )

  private val AssetIdsSet = new InternalRocksDBSet[ByteStr, MainDBColumnFamily](
    name = "asset-ids",
    columnFamily = PresetCF,
    prefix = Shorts.toByteArray(AssetIdsPrefix),
    itemEncoder = (_: ByteStr).arr,
    itemDecoder = ByteStr(_),
    keyConstructors = MainDBKey
  )

  def apply(rw: MainReadWriteDB): Unit = {
    val policySize = calculateSize(rw, PolicyIdsSet.startTarget, PrivacyCF)
    rw.put(PolicyIdsSet.sizeKey, Some(policySize))

    val assetsSize = calculateSize(rw, AssetIdsSet.startTarget, PresetCF)
    rw.put(AssetIdsSet.sizeKey, Some(assetsSize))
  }

  private def calculateSize(rw: MainReadWriteDB, start: Array[Byte], columnFamily: MainDBColumnFamily): Int =
    withResource(rw.iterator(columnFamily)) { iterator =>
      iterator.seek(start)

      if (iterator.isValid && iterator.key.sameElements(start))
        iterator.next()

      var count = 0
      while (iterator.isValid && iterator.key.startsWith(start)) {
        count += 1
        iterator.next()
      }
      count
    }
}
