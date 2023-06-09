package com.wavesenterprise.database.keys

import com.wavesenterprise.database.KeyHelpers.bytes
import com.wavesenterprise.database.rocksdb.ColumnFamily.LeaseCF
import com.wavesenterprise.database.rocksdb.RocksDBStorage
import com.wavesenterprise.database.{Key, RocksDBDeque}
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.state.{ByteStr, LeaseId}

object LeaseCFKeys {

  val LeaseDetailsPrefix: Short     = 1
  val LeasesForAddressPrefix: Short = 2

  def leaseDetails(leaseId: LeaseId): Key[Option[LeaseDetails]] = {
    Key.opt(
      "lease-details",
      LeaseCF,
      bytes(LeaseDetailsPrefix, leaseId.arr),
      LeaseDetails.fromBytes,
      (leaseDetails: LeaseDetails) => leaseDetails.toBytes
    )
  }

  def leasesForAddress(addressId: BigInt, storage: RocksDBStorage): RocksDBDeque[LeaseId] = {
    new RocksDBDeque[LeaseId](
      name = "leases-for-address",
      columnFamily = LeaseCF,
      prefix = bytes(LeasesForAddressPrefix, addressId.toByteArray),
      storage = storage,
      itemEncoder = _.arr,
      itemDecoder = bytes => LeaseId(ByteStr(bytes))
    )
  }

  def leasesForContract(contractStateId: BigInt, storage: RocksDBStorage): RocksDBDeque[LeaseId] = {
    new RocksDBDeque[LeaseId](
      name = "leases-for-contract",
      columnFamily = LeaseCF,
      prefix = bytes(LeasesForAddressPrefix, contractStateId.toByteArray),
      storage = storage,
      itemEncoder = _.arr,
      itemDecoder = bytes => LeaseId(ByteStr(bytes))
    )
  }

}
