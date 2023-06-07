package com.wavesenterprise.database.migration

import com.wavesenterprise.account.Address
import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.keys.LeaseCFKeys
import com.wavesenterprise.database.migration.MigrationV10Test.LeaseStatusInfo
import com.wavesenterprise.state.{Account, LeaseId}
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.transaction.docker.ContractTransactionGen
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import com.wavesenterprise.{TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MigrationV10Test extends AnyFreeSpec with Matchers with WithDB with TransactionGen with ContractTransactionGen {

  private val height              = 2
  private val leaseTxsCount       = 20
  private val leaseCancelTxsCount = leaseTxsCount / 2

  private val stateGen: Gen[List[(LeaseTransaction, LeaseCancelTransaction)]] = {
    Gen.listOfN(leaseTxsCount,
                leaseAndCancelGen.map {
                  case (lease: LeaseTransaction, _, leaseCancel: LeaseCancelTransaction, _) => (lease, leaseCancel)
                })
  }
  override protected def migrateScheme: Boolean = false

  def combineIterables[K, V](a: Map[K, Iterable[V]], b: Map[K, Iterable[V]]): Map[K, Iterable[V]] = {
    a ++ b.map { case (k, v) => k -> (v ++ a.getOrElse(k, Iterable.empty)) }
  }

  "MigrationV10 should work correctly" in {
    val txs            = stateGen.sample.get
    val leaseTxs       = txs.map(_._1)
    val leaseCancelTxs = txs.take(leaseCancelTxsCount).map(_._2)

    leaseTxs.foreach { leaseTx =>
      val leaseId = LeaseId(leaseTx.id())
      storage.put(Keys.transactionInfo(leaseTx.id()), Some((height, leaseTx)))
      storage.put(MigrationV10.RocksDBKeys.leaseStatusHistory(leaseId), Seq(2))
      storage.put(MigrationV10.RocksDBKeys.leaseStatus(leaseId)(height), true)

      val senderAddress    = leaseTx.sender.toAddress
      val recipientAddress = leaseTx.recipient match { case address: Address => address }
      val lastAddressIdKey = Keys.lastAddressId

      if (storage.get(Keys.addressId(senderAddress)).isEmpty) {
        val addressId = storage.get(lastAddressIdKey).getOrElse(BigInt(0)) + BigInt(1)
        storage.put(Keys.addressId(senderAddress), Some(addressId))
        storage.put(Keys.idToAddress(addressId), senderAddress)
        storage.put(lastAddressIdKey, Some(addressId))
        val addressSeqNr = storage.get(Keys.addressTransactionSeqNr(addressId)) + 1
        storage.put(Keys.addressTransactionSeqNr(addressId), addressSeqNr)
        storage.put(Keys.addressTransactionIds(addressId, addressSeqNr), Seq(leaseTx.builder.typeId.toInt -> leaseTx.id()))
      }

      if (storage.get(Keys.addressId(recipientAddress)).isEmpty) {
        val addressId = storage.get(lastAddressIdKey).getOrElse(BigInt(0)) + BigInt(1)
        storage.put(Keys.addressId(recipientAddress), Some(addressId))
        storage.put(Keys.idToAddress(addressId), recipientAddress)
        storage.put(lastAddressIdKey, Some(addressId))
        val addressSeqNr = storage.get(Keys.addressTransactionSeqNr(addressId)) + 1
        storage.put(Keys.addressTransactionSeqNr(addressId), addressSeqNr)
        storage.put(Keys.addressTransactionIds(addressId, addressSeqNr), Seq(leaseTx.builder.typeId.toInt -> leaseTx.id()))
      }

    }

    leaseCancelTxs.foreach { leaseCancelTx =>
      val leaseId = LeaseId(leaseCancelTx.leaseId)
      storage.put(Keys.transactionInfo(leaseCancelTx.id()), Some((height + 1, leaseCancelTx)))
      storage.put(MigrationV10.RocksDBKeys.leaseStatusHistory(leaseId), Seq(2, 3))
      storage.put(MigrationV10.RocksDBKeys.leaseStatus(leaseId)(height), false)
    }

    val schemaManager = new SchemaManager(storage)
    schemaManager.applyMigrations(List(MigrationType.`10`)) shouldBe 'right

    val leasesBySender = leaseTxs.groupBy(_.sender.toAddress)
    val leasesByRecipient = leaseTxs.groupBy { leaseTx =>
      leaseTx.recipient match {
        case address: Address => address
      }
    }

    val leaseFinalStatusInfos = for ((leaseTx, idx) <- leaseTxs.zipWithIndex) yield {
      if (idx < leaseCancelTxsCount) {
        LeaseId(leaseTx.id()) -> LeaseStatusInfo(leaseTx, isActive = false)
      } else {
        LeaseId(leaseTx.id()) -> LeaseStatusInfo(leaseTx, isActive = true)
      }
    }

    leaseFinalStatusInfos.foreach { case (leaseId, leaseStatusInfo) =>
      val actualLeaseDetails   = storage.get(LeaseCFKeys.leaseDetails(leaseId)).get
      val expectedLeaseDetails = leaseStatusInfo.toLeaseDetails(height)

      actualLeaseDetails shouldBe expectedLeaseDetails
    }

    val leasesByAddress = combineIterables(leasesBySender, leasesByRecipient)

    leasesByAddress.foreach { case (address, leases) =>
      val addressId                = storage.get(Keys.addressId(address)).get
      val actualLeasesForAddress   = LeaseCFKeys.leasesForAddress(addressId, storage).toList
      val expectedLeasesForAddress = leases.map(leaseTx => LeaseId(leaseTx.id()))

      actualLeasesForAddress should contain theSameElementsAs expectedLeasesForAddress
    }

  }
}

object MigrationV10Test {
  private case class LeaseStatusInfo(leaseTx: LeaseTransaction, isActive: Boolean) {
    def toLeaseDetails(height: Int): LeaseDetails = LeaseDetails(
      Account(leaseTx.sender.toAddress),
      leaseTx.recipient,
      height,
      leaseTx.amount,
      isActive = isActive,
      leaseTxId = None
    )
  }
}
