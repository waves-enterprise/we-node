package com.wavesenterprise.state.diffs

import com.google.common.base.Charsets
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.acl.PermissionsGen
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptTransactionV1}
import com.wavesenterprise.transaction.{GenesisPermitTransaction, GenesisTransaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SetScriptTransactionDiffTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val fs = TestFunctionalitySettings.Enabled.copy(preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0))

  val preconditionsAndSetScript: Gen[(GenesisTransaction, SetScriptTransaction)] = for {
    master <- accountGen
    ts     <- timestampGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    fee    <- smallFeeGen
    script <- Gen.option(scriptGen)
  } yield
    (genesis,
     SetScriptTransactionV1
       .selfSigned(AddressScheme.getAddressSchema.chainId, master, script, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], fee, ts)
       .explicitGet())

  property("setting script results in account state") {
    forAll(preconditionsAndSetScript) {
      case (genesis, setScript) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(genesis))), TestBlock.create(Seq(setScript)), fs) {
          case (_, newState) =>
            newState.accountScript(setScript.sender.toAddress) shouldBe setScript.script
        }
    }
  }

  /**
    * It's important to align Permit's and SetScript's timestamps so the former is earlier than the latter
    */
  val preconditionsWithGenesisPermit: Gen[(GenesisTransaction, GenesisPermitTransaction, SetScriptTransaction)] =
    for {
      txs <- preconditionsAndSetScript
      (genesisTx, setScriptTx) = txs
      role <- PermissionsGen.roleGen
      genesisPermitTx = GenesisPermitTransaction.create(setScriptTx.sender.toAddress, role, setScriptTx.timestamp - 1).explicitGet()
    } yield (genesisTx, genesisPermitTx, setScriptTx)

  property("script cannot be set to an account with roles") {
    forAll(preconditionsWithGenesisPermit) {
      case (genesisTx, genesisPermitTx, setScriptTx) =>
        assertDiffEi(Seq(TestBlock.create(Seq(genesisTx, genesisPermitTx))), TestBlock.create(Seq(setScriptTx)), fs) { result =>
          result should produce("Script cannot be assigned to an account with active roles!")
        }
    }
  }
}
