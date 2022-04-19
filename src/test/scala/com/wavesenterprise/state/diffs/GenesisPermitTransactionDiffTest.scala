package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.acl.Role
import com.wavesenterprise.settings.{GenesisSettingsVersion, TestBlockchainSettings}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GenesisPermitTransactionDiffTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {
  private val genesisSettings = TestBlockchainSettings.Default.custom.genesis.toPlainSettingsUnsafe

  property("Doesn't allow height other than 1st") {
    forAll(genesisPermitTxGen, Gen.chooseNum(2, Int.MaxValue)) { (genesisPermitTx, height) =>
      GenesisPermitTransactionDiff(genesisSettings, height)(genesisPermitTx) should produce(
        "GenesisPermitTransaction cannot appear in non-initial block")
    }
  }

  property("Allows height 1") {
    forAll(genesisPermitTxGen) { genesisPermitTx =>
      GenesisPermitTransactionDiff(genesisSettings, height = 1)(genesisPermitTx) shouldBe 'right
    }
  }

  property("Disallows genesis permit tx with sender role if sender role disabled in genesis") {
    forAll(genesisPermitTxGen) { genesisPermitTx =>
      val permitTxWithSenderRole = genesisPermitTx.copy(role = Role.Sender)

      GenesisPermitTransactionDiff(genesisSettings, 1)(permitTxWithSenderRole) should produce(
        "The role 'Sender' is not allowed by network parameters"
      )
    }
  }

  property("Allows genesis permit tx with sender role if sender role enabled in genesis") {
    val genesisSettingsWithEnabledSender = genesisSettings.copy(
      senderRoleEnabled = true,
      version = GenesisSettingsVersion.ModernVersion
    )

    forAll(genesisPermitTxGen) { genesisPermitTx =>
      val permitTxWithSenderRole = genesisPermitTx.copy(role = Role.Sender)
      GenesisPermitTransactionDiff(genesisSettingsWithEnabledSender, height = 1)(permitTxWithSenderRole) shouldBe 'right
    }
  }

  property("Produced Diffs combination is right") {
    forAll(genesisPermitTxGen, genesisPermitTxGen) { (tx1, tx2Temp) =>
      whenever(tx1.role != tx2Temp.role) {
        val tx2        = tx2Temp.copy(target = tx1.target)
        val diff1      = GenesisPermitTransactionDiff(genesisSettings, height = 1)(tx1).explicitGet()
        val diff2      = GenesisPermitTransactionDiff(genesisSettings, height = 1)(tx2).explicitGet()
        val resultDiff = diff1.combine(diff2)

        resultDiff.permissions(tx1.target).toSeq.length shouldBe 2
      }
    }
  }
}
