package com.wavesenterprise.database.migration

import com.wavesenterprise.account.Address
import com.wavesenterprise.database.KeyHelpers.{bytes, hBytes, historyKey}
import com.wavesenterprise.database.Keys.{LeaseStatusHistoryPrefix, LeaseStatusPrefix}
import com.wavesenterprise.database.address.AddressTransactions
import com.wavesenterprise.database.keys.LeaseCFKeys
import com.wavesenterprise.database.keys.LeaseCFKeys.LeasesForAddressPrefix
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.LeaseCF
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadWriteDB}
import com.wavesenterprise.database.{InternalRocksDBDeque, Keys, MainDBKey}
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.state.{ByteStr, LeaseId}
import com.wavesenterprise.transaction.lease.LeaseTransaction

object MigrationV10 {

  def apply(rw: MainReadWriteDB): Unit = {
    val lastAddressId = rw.get(Keys.lastAddressId).getOrElse(BigInt(0))

    for {
      addressId <- BigInt(1) to lastAddressId
      address                 = rw.get(Keys.idToAddress(addressId))
      rocksDBLeasesForAddress = RocksDBKeys.leasesForAddress(addressId)
    } {

      val addressLeaseIds = for {
        (h, leaseTx) <- leasesForAddress(rw, address).fold(msg => throw new RuntimeException(msg), identity)
        leaseId       = LeaseId(leaseTx.id())
        isLeaseActive = getLeaseStatus(rw, leaseId)
      } yield {
        val leaseDetails = LeaseDetails.fromLeaseTx(leaseTx, h).copy(isActive = isLeaseActive)
        rw.put(LeaseCFKeys.leaseDetails(leaseId), Some(leaseDetails))
        deleteLeaseStatusData(rw, leaseId)
        leaseId
      }

      rocksDBLeasesForAddress.addLastN(rw, addressLeaseIds)
    }
  }

  def leasesForAddress(rw: MainReadWriteDB, address: Address): Either[String, Seq[(Int, LeaseTransaction)]] = {
    AddressTransactions(rw, address, Set(LeaseTransaction.typeId), Int.MaxValue, None)
      .map(_.collect { case (h, tx: LeaseTransaction) => (h, tx) })
  }

  private def getLeaseStatus(rw: MainReadWriteDB, leaseId: LeaseId): Boolean = {
    rw.get(RocksDBKeys.leaseStatusHistory(leaseId)).headOption.fold(false)(h => rw.get(RocksDBKeys.leaseStatus(leaseId)(h)))
  }

  private def deleteLeaseStatusData(rw: MainReadWriteDB, leaseId: LeaseId): Unit = {
    val leaseStatusHistoryHeights = rw.get(RocksDBKeys.leaseStatusHistory(leaseId))
    rw.delete(RocksDBKeys.leaseStatusHistory(leaseId))
    leaseStatusHistoryHeights.foreach(height => rw.delete(RocksDBKeys.leaseStatus(leaseId)(height)))
  }

  object RocksDBKeys {
    def leaseStatusHistory(leaseId: LeaseId): MainDBKey[Seq[Int]] = historyKey("lease-status-history", LeaseStatusHistoryPrefix, leaseId.arr)

    def leaseStatus(leaseId: LeaseId)(height: Int): MainDBKey[Boolean] =
      MainDBKey("lease-status", hBytes(LeaseStatusPrefix, height, leaseId.arr), _(0) == 1, active => Array[Byte](if (active) 1 else 0))

    def leasesForAddress(addressId: BigInt): InternalRocksDBDeque[LeaseId, MainDBColumnFamily] = {
      new InternalRocksDBDeque(
        name = "leases-for-address",
        columnFamily = LeaseCF,
        prefix = bytes(LeasesForAddressPrefix, addressId.toByteArray),
        MainDBKey,
        itemEncoder = _.arr,
        itemDecoder = bytes => LeaseId(ByteStr(bytes))
      )
    }
  }

}
