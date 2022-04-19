package com.wavesenterprise.database.migration

import com.wavesenterprise.database.migration.MigrationV2.{KeysInfo, LegacyAssetInfo, LegacyContractInfo, ModernContractInfo}
import com.wavesenterprise.database.{Keys, WEKeys}
import com.wavesenterprise.state.AssetInfo
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.assets.IssueTransaction
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, CreateContractTransaction}
import com.wavesenterprise.{TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.{FreeSpec, Matchers}

import java.nio.charset.StandardCharsets.UTF_8

class MigrationV2Test extends FreeSpec with Matchers with WithDB with ContractTransactionGen with TransactionGen {

  private val height = 2
  private val count  = 20

  private val stateGen: Gen[List[Transaction]] = {
    Gen.listOfN(count, Gen.oneOf(issueGen, createContractV2ParamGen))
  }

  override protected def migrateScheme: Boolean = false

  private def getSchemaManager: SchemaManager = new SchemaManager(storage)

  "MigrationV2 should work correctly" in {
    val txs = stateGen.sample.get

    txs.foreach { tx =>
      storage.put(Keys.transactionInfo(tx.id()), Some((height, tx)))

      tx match {
        case issueTx: IssueTransaction =>
          val lastAddressId = storage.get(Keys.lastAddressId).getOrElse(BigInt(0)) + 1
          storage.put(Keys.assetList(lastAddressId), Set(issueTx.assetId()))
          storage.put(Keys.lastAddressId, Some(lastAddressId))
          storage.put(Keys.assetInfoHistory(issueTx.assetId()), Seq(height))
          storage.put(KeysInfo.assetInfoKey(issueTx.assetId())(height), LegacyAssetInfo(issueTx.reissuable, issueTx.quantity))
        case createTx: CreateContractTransaction =>
          WEKeys.contractIdsSet(storage).add(createTx.contractId)
          storage.put(WEKeys.contractHistory(createTx.contractId), Seq(height))
          storage.put(
            KeysInfo.legacyContractInfoKey(createTx.contractId)(height),
            Some(LegacyContractInfo(createTx.contractId, createTx.image, createTx.imageHash, 1, active = true))
          )
      }
    }

    val schemaManager = getSchemaManager
    schemaManager.applyMigrations(List(MigrationType.`1`, MigrationType.`2`)).left.foreach(ex => throw ex)

    txs.foreach {
      case issueTx: IssueTransaction =>
        storage.get(Keys.assetInfo(issueTx.assetId())(height)) shouldBe AssetInfo(
          issueTx.sender,
          height,
          issueTx.timestamp,
          new String(issueTx.name, UTF_8),
          new String(issueTx.description, UTF_8),
          issueTx.decimals,
          issueTx.reissuable,
          issueTx.quantity
        )
      case createTx: CreateContractTransaction =>
        storage.get(KeysInfo.modernContractInfoKey(createTx.contractId)(height)) shouldBe Some(
          ModernContractInfo(createTx.sender, createTx.contractId, createTx.image, createTx.imageHash, 1, active = true))
    }
  }
}
