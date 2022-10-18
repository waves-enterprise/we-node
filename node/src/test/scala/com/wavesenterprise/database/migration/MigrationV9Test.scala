package com.wavesenterprise.database.migration

import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.migration.MigrationV2.{AssetInfoV2, KeysInfo}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.AssetInfo
import com.wavesenterprise.transaction.assets.IssueTransaction
import com.wavesenterprise.transaction.docker.ContractTransactionGen
import com.wavesenterprise.{TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.{FreeSpec, Matchers}

import java.nio.charset.StandardCharsets.UTF_8

class MigrationV9Test extends AnyFreeSpec with Matchers with WithDB with TransactionGen with ContractTransactionGen {

  private val height = 2
  private val count  = 20

  private val stateGen: Gen[List[IssueTransaction]] = {
    Gen.listOfN(count, issueGen)
  }
  override protected def migrateScheme: Boolean = false

  "MigrationV8 should work correctly" in {
    val issueTxs = stateGen.sample.get

    issueTxs.foreach { issueTx =>
      storage.put(Keys.transactionInfo(issueTx.id()), Some((height, issueTx)))

      val lastAddressId = storage.get(Keys.lastAddressId).getOrElse(BigInt(0)) + 1
      storage.put(Keys.assetList(lastAddressId), Set(issueTx.assetId()))
      storage.put(Keys.lastAddressId, Some(lastAddressId))
      storage.put(Keys.assetInfoHistory(issueTx.assetId()), Seq(height))
      storage.put(
        KeysInfo.assetInfoV2Key(issueTx.assetId())(height),
        AssetInfoV2(
          issueTx.sender,
          height,
          issueTx.timestamp,
          new String(issueTx.name, UTF_8),
          description = new String(issueTx.description, UTF_8),
          issueTx.decimals,
          issueTx.reissuable,
          issueTx.quantity
        )
      )
    }

    val schemaManager = new SchemaManager(storage)
    schemaManager.applyMigrations(List(MigrationType.`9`)) shouldBe 'right

    issueTxs.foreach { issueTx =>
      storage.get(Keys.assetInfo(issueTx.assetId())(height)) shouldBe AssetInfo(
        issueTx.sender.toAddress.toAssetHolder,
        height,
        issueTx.timestamp,
        new String(issueTx.name, UTF_8),
        description = new String(issueTx.description, UTF_8),
        issueTx.decimals,
        issueTx.reissuable,
        issueTx.quantity
      )

    }

  }
}
