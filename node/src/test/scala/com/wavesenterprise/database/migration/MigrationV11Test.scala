package com.wavesenterprise.database.migration

import com.wavesenterprise.database.migration.MigrationV11.ModernContractInfo
import com.wavesenterprise.database.{Keys, WEKeys}
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, CreateContractTransactionV2}
import com.wavesenterprise.{TransactionGen, WithDB}
import monix.eval.Coeval
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import tools.GenHelper._

class MigrationV11Test extends AnyFreeSpec with Matchers with WithDB with TransactionGen with ContractTransactionGen {

  private val height = 2
  private val count  = 20

  private val stateGen: Gen[List[CreateContractTransactionV2]] =
    Gen.listOfN(count, createContractV2ParamGen)

  override protected def migrateScheme: Boolean = false

  "MigrationV11 should work correctly" in {
    val txs = stateGen.generateSample()

    txs.foreach { createTx =>
      storage.put(Keys.transactionInfo(createTx.id()), Some((height, createTx)))

      WEKeys.contractIdsSet(storage).add(createTx.contractId)
      storage.put(WEKeys.contractHistory(createTx.contractId), Seq(height))
      storage.put(
        MigrationV2.KeysInfo.legacyContractInfoKey(createTx.contractId)(height),
        Some(
          MigrationV2.LegacyContractInfo(
            createTx.contractId,
            createTx.image,
            createTx.imageHash,
            1,
            active = true
          ))
      )
    }

    val schemaManager = new MainSchemaManager(storage)
    schemaManager.applyMigrations(List(MainMigrationType.`1`,
                                       MainMigrationType.`2`,
                                       MainMigrationType.`7`,
                                       MainMigrationType.`10`,
                                       MainMigrationType.`11`)).left.foreach(ex => throw ex)

    txs.foreach { createTx =>
      storage.get(MigrationV11.KeysInfo.modernContractInfoKey(createTx.contractId)(height)) shouldBe Some(
        ModernContractInfo(
          creator = Coeval.pure(createTx.sender),
          contractId = createTx.contractId,
          image = createTx.image,
          imageHash = createTx.imageHash,
          version = 1,
          active = true,
          validationPolicy = ValidationPolicy.Default,
          apiVersion = ContractApiVersion.Initial,
          groupParticipants = Set.empty,
          groupOwners = Set.empty
        ))
    }
  }
}
