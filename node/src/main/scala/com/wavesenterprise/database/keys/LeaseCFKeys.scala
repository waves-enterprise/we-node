package com.wavesenterprise.database.keys

import com.wavesenterprise.database.KeyHelpers.bytes
import com.wavesenterprise.database.RocksDBDeque.MainRocksDBDeque
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.LeaseCF
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.database.{MainDBKey, RocksDBDeque}
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.state.{ByteStr, LeaseId}

object LeaseCFKeys {

  val LeaseDetailsPrefix: Short     = 1
  val LeasesForAddressPrefix: Short = 2

  def leaseDetails(leaseId: LeaseId): MainDBKey[Option[LeaseDetails]] = {
    MainDBKey.opt(
      "lease-details",
      LeaseCF,
      bytes(LeaseDetailsPrefix, leaseId.arr),
      LeaseDetails.fromBytes,
      (leaseDetails: LeaseDetails) => leaseDetails.toBytes
    )
  }

  def leasesForAddress(addressId: BigInt, storage: MainRocksDBStorage): MainRocksDBDeque[LeaseId] = {
    RocksDBDeque.newMain[LeaseId](
      name = "leases-for-address",
      columnFamily = LeaseCF,
      prefix = bytes(LeasesForAddressPrefix, addressId.toByteArray),
      storage = storage,
      itemEncoder = (_: LeaseId).arr,
      itemDecoder = (bytes: Array[Byte]) => LeaseId(ByteStr(bytes))
    )
  }

  def leasesForContract(contractStateId: BigInt, storage: MainRocksDBStorage): MainRocksDBDeque[LeaseId] = {
    RocksDBDeque.newMain[LeaseId](
      name = "leases-for-contract",
      columnFamily = LeaseCF,
      prefix = bytes(LeasesForAddressPrefix, contractStateId.toByteArray),
      storage = storage,
      itemEncoder = (_: LeaseId).arr,
      itemDecoder = (bytes: Array[Byte]) => LeaseId(ByteStr(bytes))
    )
  }

}
