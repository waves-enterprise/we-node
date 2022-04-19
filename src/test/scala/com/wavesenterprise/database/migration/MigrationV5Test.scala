package com.wavesenterprise.database.migration

import com.wavesenterprise.WithDB
import com.wavesenterprise.account.Address
import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.settings.{TestFunctionalitySettings, WESettings}
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.DataTransaction
import com.wavesenterprise.transaction.docker.ContractTransactionGen
import org.scalacheck.Gen
import org.scalatest.{FreeSpec, Matchers}

class MigrationV5Test extends FreeSpec with Matchers with WithDB with ContractTransactionGen {

  private val count = 20

  private val stateGen: Gen[List[DataTransaction]] = {
    Gen.listOfN(count, dataTransactionV1Gen)
  }

  private def createWESettings(): WESettings = {
    val custom = DefaultWESettings.blockchain.custom.copy(functionality = TestFunctionalitySettings.Enabled)
    DefaultWESettings.copy(blockchain = DefaultWESettings.blockchain.copy(custom = custom))
  }

  override protected def migrateScheme: Boolean = false

  private def getSchemaManager: SchemaManager = new SchemaManager(storage)

  "MigrationV5 should work correctly" in {
    val schemaManager = getSchemaManager
    schemaManager.applyMigrations(List(MigrationType.`1`, MigrationType.`2`, MigrationType.`3`, MigrationType.`4`))

    val txs = stateGen.sample.get

    val dataKeys = txs.foldLeft(Map.empty[Address, List[DataEntry[_]]]) { (acc, tx) =>
      val lastAddressIdKey = Keys.lastAddressId
      val addressId        = storage.get(lastAddressIdKey).getOrElse(BigInt(0)) + BigInt(1)
      val address          = tx.sender.toAddress
      storage.put(Keys.addressId(address), Some(addressId))
      storage.put(Keys.idToAddress(addressId), address)
      storage.put(lastAddressIdKey, Some(addressId))

      val chunkCountKey = MigrationV5.LegacyKeys.dataKeyChunkCount(addressId)
      val chunkCount    = storage.get(chunkCountKey)
      val chunkKey      = MigrationV5.LegacyKeys.dataKeyChunk(addressId, chunkCount)
      val chunkKeys     = tx.data.map(_.key)
      storage.put(chunkKey, chunkKeys)
      storage.put(chunkCountKey, chunkCount + 1)
      tx.data.foreach { e =>
        storage.put(Keys.dataHistory(addressId, e.key), Seq(1))
        storage.put(Keys.data(addressId, e.key)(1), Some(e))
      }
      acc + (address -> tx.data)
    }

    schemaManager.applyMigrations(List(MigrationType.`5`))

    val settings = createWESettings()
    val dbWriter = RocksDBWriter(storage, settings)

    dataKeys.foreach {
      case (address, expected) =>
        expected should contain theSameElementsAs dbWriter.accountData(address).data.values
    }
  }
}
