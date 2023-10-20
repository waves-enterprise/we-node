package com.wavesenterprise.database.migration

import com.wavesenterprise.database.migration.MigrationV2.{KeysInfo, LegacyContractInfo}
import com.wavesenterprise.database.{Keys, WEKeys}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, CreateContractTransactionV2}
import com.wavesenterprise.{TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import tools.GenHelper._

class MigrationV7Test extends AnyFreeSpec with Matchers with WithDB with ContractTransactionGen with TransactionGen {

  private val height = 2
  private val count  = 20

  private val stateGen: Gen[List[CreateContractTransactionV2]] =
    Gen.listOfN(count, createContractV2ParamGen)

  override protected def migrateScheme: Boolean = false

  private def getSchemaManager: MainSchemaManager = new MainSchemaManager(storage)

  "MigrationV7 should work correctly" in {
    val txs = stateGen.generateSample()

    txs.foreach { createTx =>
      storage.put(Keys.transactionInfo(createTx.id()), Some((height, createTx)))

      WEKeys.contractIdsSet(storage).add(createTx.contractId)
      storage.put(WEKeys.contractHistory(createTx.contractId), Seq(height))
      storage.put(
        KeysInfo.legacyContractInfoKey(createTx.contractId)(height),
        Some(LegacyContractInfo(createTx.contractId, createTx.image, createTx.imageHash, 1, active = true))
      )
    }

    val schemaManager = getSchemaManager
    schemaManager.applyMigrations(List(MainMigrationType.`1`, MainMigrationType.`2`, MainMigrationType.`7`)).left.foreach(ex => throw ex)

    txs.foreach { createTx =>
      storage.get(MigrationV7.KeysInfo.modernContractInfoKey(createTx.contractId)(height)) shouldBe Some(
        MigrationV7.ModernContractInfo(
          creator = createTx.sender,
          contractId = createTx.contractId,
          image = createTx.image,
          imageHash = createTx.imageHash,
          version = 1,
          active = true,
          validationPolicy = ValidationPolicy.Default,
          apiVersion = ContractApiVersion.Initial
        ))
    }
  }
}
