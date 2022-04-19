package com.wavesenterprise.state.diffs.smart.scenarios

import com.google.common.base.Charsets
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.transaction.assets.{IssueTransactionV2, SponsorFeeTransactionV1}
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.ScriptCompiler
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.nio.charset.StandardCharsets.UTF_8

class ScriptedSponsorTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  import com.wavesenterprise.state.diffs._

  val ENOUGH_FEE: Long = 100000000
  val CHAIN_ID: Byte   = AddressScheme.getAddressSchema.chainId

  val fs = TestFunctionalitySettings.Enabled
    .copy(
      preActivatedFeatures = Map(
        BlockchainFeature.NG.id                   -> 0,
        BlockchainFeature.MassTransfer.id         -> 0,
        BlockchainFeature.SmartAccounts.id        -> 0,
        BlockchainFeature.DataTransaction.id      -> 0,
        BlockchainFeature.BurnAnyTokens.id        -> 0,
        BlockchainFeature.FeeSwitch.id            -> 0,
        BlockchainFeature.SmartAssets.id          -> 0,
        BlockchainFeature.SmartAccountTrading.id  -> 0,
        BlockchainFeature.SponsoredFeesSupport.id -> 0
      ),
      featureCheckBlocksPeriod = 2,
      blocksForFeatureActivation = 1
    )

  property("sponsorship works when used by scripted accounts") {
    forAll(separateContractAndSponsor) {
      case (setupTxs, transfer) =>
        val setupBlocks   = setupTxs.map(TestBlock.create)
        val transferBlock = TestBlock.create(Seq(transfer))

        val Some(assetId) = transfer.feeAssetId
        val contract      = transfer.sender

        val contractSpent: Long = ENOUGH_FEE + 1
        val sponsorSpent: Long  = ENOUGH_FEE * 3 - 1 + ENOUGH_FEE

        val sponsor = setupTxs.flatten.collectFirst { case t: SponsorFeeTransactionV1 => t.sender }.get

        assertDiffAndState(setupBlocks :+ TestBlock.create(Nil), transferBlock, fs) { (diff, blck) =>
          blck.balance(contract.toAddress, Some(assetId)) shouldEqual ENOUGH_FEE * 2
          blck.balance(contract.toAddress) shouldEqual ENOUGH_AMT - contractSpent

          blck.balance(sponsor.toAddress, Some(assetId)) shouldEqual Long.MaxValue - ENOUGH_FEE * 2
          blck.balance(sponsor.toAddress) shouldEqual ENOUGH_AMT - sponsorSpent
        }
    }
  }

  property("sponsorship works when sponsored by scripted accounts") {
    forAll(scriptedSponsor) {
      case (setupTxs, transfer) =>
        val setupBlocks   = setupTxs.map(TestBlock.create)
        val transferBlock = TestBlock.create(Seq(transfer))

        val Some(assetId) = transfer.feeAssetId
        val contract      = setupTxs.flatten.collectFirst { case t: SponsorFeeTransactionV1 => t.sender }.get
        val recipient     = transfer.sender

        val contractSpent: Long  = ENOUGH_FEE * 4 + ENOUGH_FEE
        val recipientSpent: Long = 1

        assertDiffAndState(setupBlocks :+ TestBlock.create(Nil), transferBlock, fs) { (diff, blck) =>
          blck.balance(contract.toAddress, Some(assetId)) shouldEqual Long.MaxValue - ENOUGH_FEE * 2
          blck.balance(contract.toAddress) shouldEqual ENOUGH_AMT - contractSpent

          blck.balance(recipient.toAddress, Some(assetId)) shouldEqual ENOUGH_FEE * 2
          blck.balance(recipient.toAddress) shouldEqual ENOUGH_AMT - recipientSpent
        }
    }
  }

  val scriptedSponsor = {
    val timestamp = System.currentTimeMillis()
    for {
      contract  <- accountGen
      recipient <- accountGen
      gen1 = GenesisTransaction
        .create(contract.toAddress, ENOUGH_AMT, timestamp)
        .explicitGet()
      gen2 = GenesisTransaction
        .create(recipient.toAddress, ENOUGH_AMT, timestamp)
        .explicitGet()
      (script, _) = ScriptCompiler(s"false", isAssetScript = false).explicitGet()
      issueTx = IssueTransactionV2
        .selfSigned(
          currentChainId,
          sender = contract,
          name = "Asset#1".getBytes(UTF_8),
          description = "description".getBytes(UTF_8),
          quantity = Long.MaxValue,
          decimals = 8,
          reissuable = false,
          fee = ENOUGH_FEE,
          timestamp = timestamp + 2,
          script = None
        )
        .explicitGet()
      sponsorTx = SponsorFeeTransactionV1
        .selfSigned(
          contract,
          issueTx.id(),
          isEnabled = true,
          ENOUGH_FEE,
          timestamp + 4
        )
        .explicitGet()
      transferToRecipient = TransferTransactionV2
        .selfSigned(contract,
                    Some(issueTx.id()),
                    None,
                    System.currentTimeMillis() + 4,
                    ENOUGH_FEE * 3,
                    ENOUGH_FEE,
                    recipient.toAddress,
                    Array.emptyByteArray)
        .explicitGet()
      setScript = SetScriptTransactionV1
        .selfSigned(
          CHAIN_ID,
          contract,
          Some(script),
          "script".getBytes(Charsets.UTF_8),
          Array.empty[Byte],
          ENOUGH_FEE,
          System.currentTimeMillis() + 6
        )
        .explicitGet()
      transferTx = TransferTransactionV2
        .selfSigned(recipient,
                    None,
                    Some(issueTx.id()),
                    System.currentTimeMillis() + 8,
                    1,
                    ENOUGH_FEE,
                    accountGen.sample.get.toAddress,
                    Array.emptyByteArray)
        .explicitGet()
    } yield (Seq(Seq(gen1, gen2), Seq(issueTx, sponsorTx), Seq(transferToRecipient, setScript)), transferTx)
  }

  val separateContractAndSponsor: Gen[(Seq[Seq[Transaction]], TransferTransaction)] = {
    val timestamp = System.currentTimeMillis()
    for {
      contract <- accountGen
      sponsor  <- accountGen
      gen1 = GenesisTransaction
        .create(contract.toAddress, ENOUGH_AMT, timestamp)
        .explicitGet()
      gen2 = GenesisTransaction
        .create(sponsor.toAddress, ENOUGH_AMT, timestamp)
        .explicitGet()
      (script, _) = ScriptCompiler(s"true", isAssetScript = false).explicitGet()
      issueTx = IssueTransactionV2
        .selfSigned(
          currentChainId,
          sender = sponsor,
          name = "Asset#1".getBytes(UTF_8),
          description = "description".getBytes(UTF_8),
          quantity = Long.MaxValue,
          decimals = 8,
          reissuable = false,
          fee = ENOUGH_FEE,
          timestamp = timestamp + 2,
          script = None
        )
        .explicitGet()
      sponsorTx = SponsorFeeTransactionV1
        .selfSigned(
          sponsor,
          issueTx.id(),
          isEnabled = true,
          fee = ENOUGH_FEE,
          timestamp = timestamp + 4
        )
        .explicitGet()
      transferToContract = TransferTransactionV2
        .selfSigned(sponsor,
                    Some(issueTx.id()),
                    None,
                    System.currentTimeMillis() + 4,
                    ENOUGH_FEE * 3,
                    ENOUGH_FEE,
                    contract.toAddress,
                    Array.emptyByteArray)
        .explicitGet()
      setScript = SetScriptTransactionV1
        .selfSigned(
          CHAIN_ID,
          contract,
          Some(script),
          "script".getBytes(Charsets.UTF_8),
          Array.empty[Byte],
          ENOUGH_FEE,
          System.currentTimeMillis() + 6
        )
        .explicitGet()
      transferTx = TransferTransactionV2
        .selfSigned(contract, None, Some(issueTx.id()), System.currentTimeMillis() + 8, 1, ENOUGH_FEE, sponsor.toAddress, Array.emptyByteArray)
        .explicitGet()
    } yield (Seq(Seq(gen1, gen2), Seq(issueTx, sponsorTx), Seq(transferToContract, setScript)), transferTx)
  }
}
