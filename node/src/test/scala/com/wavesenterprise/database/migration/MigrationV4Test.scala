package com.wavesenterprise.database.migration

import com.wavesenterprise.WithDB
import com.wavesenterprise.database.WEKeys
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.settings.{TestFunctionalitySettings, WESettings}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext.Default
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, ExecutedContractTransaction}
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MigrationV4Test extends AnyFreeSpec with Matchers with WithDB with ContractTransactionGen {

  private val count = 20

  private val stateGen: Gen[List[ExecutedContractTransaction]] = {
    Gen.listOfN(count, executedContractV1ParamGen)
  }

  private def createWESettings(): WESettings = {
    val custom = DefaultWESettings.blockchain.custom.copy(functionality = TestFunctionalitySettings.Enabled)
    DefaultWESettings.copy(blockchain = DefaultWESettings.blockchain.copy(custom = custom))
  }

  override protected def migrateScheme: Boolean = false

  private def getSchemaManager: SchemaManager = new SchemaManager(storage)

  "MigrationV4 should work correctly" in {
    val schemaManager = getSchemaManager
    schemaManager.applyMigrations(List(MigrationType.`1`, MigrationType.`2`, MigrationType.`3`))

    val txs = stateGen.sample.get

    val contractKeys = txs.foldLeft(Map.empty[ByteStr, List[String]]) { (acc, tx) =>
      val contractId    = tx.tx.contractId
      val chunkCountKey = MigrationV4.LegacyKeys.contractKeyChunkCount(contractId)
      val chunkCount    = storage.get(chunkCountKey)
      val chunkKey      = MigrationV4.LegacyKeys.contractDataKeyChunk(contractId, chunkCount)
      val chunkKeys     = tx.results.map(_.key)
      storage.put(chunkKey, chunkKeys.toVector)
      storage.put(chunkCountKey, chunkCount + 1)
      chunkKeys.foreach { key =>
        storage.put(WEKeys.contractDataHistory(contractId, key), Seq(1))
      }
      acc + (contractId -> chunkKeys)
    }

    val contractsSet = WEKeys.contractIdsSet(storage)
    contractsSet.add(contractKeys.keys)

    schemaManager.applyMigrations(List(MigrationType.`4`))

    val settings = createWESettings()
    val dbWriter = RocksDBWriter(storage, settings)

    contractKeys.foreach {
      case (contractId, expected) =>
        expected should contain theSameElementsAs dbWriter.contractKeys(KeysRequest(contractId), Default)
    }
  }
}
