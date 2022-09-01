package com.wavesenterprise.database.migration

import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.settings.{TestFunctionalitySettings, WESettings}
import com.wavesenterprise.transaction.assets.IssueTransaction
import com.wavesenterprise.transaction.{Authorized, CreatePolicyTransaction, Transaction}
import com.wavesenterprise.{TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/**
  * [[MigrationV3]] test case.
  * Actually this migration is broken. Test case had been broken too and that's why it was rewritten.
  */
class MigrationV3Test extends AnyFreeSpec with Matchers with WithDB with TransactionGen {

  private val height = 2
  private val count  = 100

  private val stateGen: Gen[List[Transaction with Authorized]] = {
    Gen.listOfN(count, Gen.oneOf(issueGen, createPolicyTransactionV1Gen(5, 5).map(_.tx)))
  }

  private def createWESettings(): WESettings = {
    val custom = DefaultWESettings.blockchain.custom.copy(functionality = TestFunctionalitySettings.Enabled)
    DefaultWESettings.copy(blockchain = DefaultWESettings.blockchain.copy(custom = custom))
  }

  override protected def migrateScheme: Boolean = false

  private def getSchemaManager: SchemaManager = new SchemaManager(storage)

  "MigrationV3 should work correctly" in {
    val schemaManager = getSchemaManager
    schemaManager.applyMigrations(List(MigrationType.`1`, MigrationType.`2`)).left.foreach(ex => throw ex)

    val txs = stateGen.sample.get

    txs.foreach { tx =>
      storage.put(Keys.transactionInfo(tx.id()), Some((height, tx)))
      val address   = tx.sender.toAddress
      val addressId = storage.get(Keys.lastAddressId).getOrElse(BigInt(0)) + 1
      storage.put(Keys.addressId(address), Some(addressId))
      storage.put(Keys.idToAddress(addressId), address)
      storage.put(Keys.lastAddressId, Some(addressId))
      val addressSeqNr = storage.get(Keys.addressTransactionSeqNr(addressId)) + 1
      storage.put(Keys.addressTransactionSeqNr(addressId), addressSeqNr)
      storage.put(Keys.addressTransactionIds(addressId, addressSeqNr), Seq(tx.builder.typeId.toInt -> tx.id()))
    }

    /* Applying broken migration */
    schemaManager.applyMigrations(List(MigrationType.`3`)).left.foreach(ex => throw ex)

    val assets   = txs.filter(_.builder.typeId == IssueTransaction.typeId).map(_.id())
    val policies = txs.filter(_.builder.typeId == CreatePolicyTransaction.typeId).map(_.id())

    val settings = createWESettings()
    val dbWriter = RocksDBWriter(storage, settings)

    /* Assets and polices from state should contain 1 element because of wrong usage of RocksDBSet update in MigrationV3 */
    dbWriter.assets() should contain oneElementOf assets
    dbWriter.policies() should contain oneElementOf policies
  }
}
