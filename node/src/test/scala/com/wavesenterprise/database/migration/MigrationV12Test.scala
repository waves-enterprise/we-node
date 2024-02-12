package com.wavesenterprise.database.migration

import com.wavesenterprise.database.migration.MigrationV12.{KeysInfo, LegacyContractInfo}
import com.wavesenterprise.database.{Keys, WEKeys}
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, CreateContractTransactionV4}
import com.wavesenterprise.{TransactionGen, WithDB}
import monix.eval.Coeval
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import tools.GenHelper._

class MigrationV12Test extends AnyFreeSpec with Matchers with WithDB with ContractTransactionGen with TransactionGen {

  private val height = 2
  private val count  = 20

  private val stateGen: Gen[List[CreateContractTransactionV4]] =
    Gen.listOfN(count, createContractV4ParamGen(atomicBadgeOptGen, validationPolicyGen, contractApiVersionGen))

  override protected def migrateScheme: Boolean = false

  private def getSchemaManager: MainSchemaManager = new MainSchemaManager(storage)

  "MigrationV12 should work correctly" in {
    val txs           = stateGen.generateSample()
    val schemaManager = getSchemaManager
    schemaManager.applyMigrations(List(MainMigrationType.`1`,
                                       MainMigrationType.`2`,
                                       MainMigrationType.`7`,
                                       MainMigrationType.`10`,
                                       MainMigrationType.`11`)).left.foreach(ex => throw ex)

    txs.foreach { createTx =>
      storage.put(Keys.transactionInfo(createTx.id()), Some((height, createTx)))

      WEKeys.contractIdsSet(storage).add(createTx.contractId)
      storage.put(WEKeys.contractHistory(createTx.contractId), Seq(height))
      storage.put(
        KeysInfo.legacyContractInfoKey(createTx.contractId)(height),
        Some(LegacyContractInfo(
          createTx.sender,
          createTx.contractId,
          createTx.image,
          createTx.imageHash,
          1,
          active = true,
          createTx.validationPolicy,
          createTx.apiVersion
        ))
      )
    }
    schemaManager.applyMigrations(List(MainMigrationType.`12`)).left.foreach(ex => throw ex)
    txs.foreach { createTx =>

      storage.get(MigrationV12.KeysInfo.modernContractInfoKey(createTx.contractId)(height)) shouldBe Some(
        ContractInfo(
          creator = Coeval.pure(createTx.sender),
          contractId = createTx.contractId,
          storedContract = DockerContract(createTx.image, createTx.imageHash, createTx.apiVersion),
          version = 1,
          active = true,
          validationPolicy = createTx.validationPolicy
        ))
    }
  }
}
