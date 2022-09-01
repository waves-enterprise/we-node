package com.wavesenterprise.database.migration

import com.wavesenterprise.database.Keys
import com.wavesenterprise.{TransactionGen, WithDB}
import org.scalacheck.Gen
import tools.GenHelper._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MigrationV8Test extends AnyFreeSpec with Matchers with WithDB with TransactionGen {
  override protected def migrateScheme: Boolean = false

  "MigrationV8 should work correctly" in {
    val height = 2
    val txs    = Gen.listOfN(30, createAliasV2Gen).generateSample()

    val addressToId = txs.zipWithIndex.map {
      case (tx, i) =>
        val addressId = BigInt(i + 1)
        val sender    = tx.sender.toAddress
        storage.put(Keys.idToAddress(addressId), sender)
        storage.put(Keys.addressId(sender), Some(addressId))
        storage.put(Keys.transactionInfo(tx.id()), Some((height, tx)))

        val kk        = Keys.addressTransactionSeqNr(addressId)
        val nextSeqNr = storage.get(kk) + 1
        val txsIds    = Seq((tx.builder.typeId.toInt -> tx.id()))
        storage.put(Keys.addressTransactionIds(addressId, nextSeqNr), txsIds)
        storage.put(kk, nextSeqNr)
        sender -> addressId
    }.toMap

    val lastAddressId = BigInt(txs.size)
    storage.put(Keys.lastAddressId, Some(lastAddressId))

    val aliasesByAddress = txs.groupBy(_.sender.toAddress).mapValues(_.map(_.alias).toSet)

    val schemaManager = new SchemaManager(storage)
    schemaManager.applyMigrations(List(MigrationType.`8`)) shouldBe 'right

    aliasesByAddress.foreach {
      case (address, aliases) =>
        val addressId          = addressToId(address)
        val aliasesFromStorage = Keys.issuedAliasesByAddressId(addressId, storage).members
        aliases should contain theSameElementsAs aliasesFromStorage
    }
  }
}
