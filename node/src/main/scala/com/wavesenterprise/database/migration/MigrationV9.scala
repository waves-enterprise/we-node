package com.wavesenterprise.database.migration

import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.migration.MigrationV2.KeysInfo
import com.wavesenterprise.database.rocksdb.RW
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.AssetInfo
import com.wavesenterprise.transaction.AssetId

/**
  * WE-8198: changed AssetInfo/AssetDescription model
  */
object MigrationV9 {

  def apply(rw: RW): Unit = {
    val lastAddressId = rw.get(Keys.lastAddressId).getOrElse(BigInt(0))
    val assetsBuilder = Set.newBuilder[AssetId]
    for (id <- BigInt(1) to lastAddressId) {
      assetsBuilder ++= rw.get(Keys.assetList(id))
    }
    val assets = assetsBuilder.result()

    for {
      assetId <- assets
      assetHistory = rw.get(Keys.assetInfoHistory(assetId))
      height <- assetHistory
      oldAssetInfo = rw.get(KeysInfo.assetInfoV2Key(assetId)(height))
    } yield {
      val newAssetInfo = AssetInfo(
        issuer = oldAssetInfo.issuer.toAddress.toAssetHolder,
        height = oldAssetInfo.height,
        timestamp = oldAssetInfo.timestamp,
        name = oldAssetInfo.name,
        description = oldAssetInfo.description,
        decimals = oldAssetInfo.decimals,
        reissuable = oldAssetInfo.reissuable,
        volume = oldAssetInfo.volume
      )

      rw.put(Keys.assetInfo(assetId)(height), newAssetInfo)
    }
  }
}
