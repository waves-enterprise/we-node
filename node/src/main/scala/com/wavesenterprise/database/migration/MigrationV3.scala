package com.wavesenterprise.database.migration

import com.google.common.primitives.Shorts
import com.wavesenterprise.account.Address
import com.wavesenterprise.database.Keys.AssetIdsPrefix
import com.wavesenterprise.database.address.AddressTransactions
import com.wavesenterprise.database.keys.PrivacyCFKeys.PolicyIdsPrefix
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.{PresetCF, PrivacyCF}
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import com.wavesenterprise.database.{InternalRocksDBSet, Keys, MainDBKey}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.CreatePolicyTransaction
import com.wavesenterprise.transaction.assets.IssueTransaction

object MigrationV3 {

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
    val lastAddressId = rw.get(Keys.lastAddressId).getOrElse(BigInt(0))
    for (id <- BigInt(1) to lastAddressId) {
      val address = rw.get(Keys.idToAddress(id))
      migrateAssets(rw, address)
      migratePolicies(rw, address)
    }
  }

  private def migratePolicies(rw: MainReadWriteDB, address: Address): Unit = {
    val policies =
      AddressTransactions.takeTxIds(rw, address, Set(CreatePolicyTransaction.typeId), Int.MaxValue, None).right.get.toSet
    if (policies.nonEmpty) {
      PolicyIdsSet.add(rw, policies)
    }
  }

  private def migrateAssets(rw: MainReadWriteDB, address: Address): Unit = {
    val assets = AddressTransactions.takeTxIds(rw, address, Set(IssueTransaction.typeId), Int.MaxValue, None).right.get.toSet
    if (assets.nonEmpty) {
      AssetIdsSet.add(rw, assets)
    }
  }
}
