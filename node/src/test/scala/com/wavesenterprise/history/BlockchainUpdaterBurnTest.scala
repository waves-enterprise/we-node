package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.Role
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.settings.{BlockchainSettings, WESettings}
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, produce}
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV2}
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockchainUpdaterBurnTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  val West: Long = 100000000

  type Setup =
    (Long, Seq[Transaction], TransferTransaction, IssueTransaction, BurnTransaction, ReissueTransaction)

  val preconditions: Gen[Setup] = for {
    master                                                   <- accountGen
    ts                                                       <- timestampGen
    transferAssetWestFee                                     <- smallFeeGen
    alice                                                    <- accountGen
    (_, assetName, description, quantity, decimals, _, _, _) <- issueParamGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    genesisPermitTxs = buildGenesisPermitTxs(ts,
                                             Map(
                                               defaultSigner.toAddress -> Role.Miner,
                                               alice.toAddress         -> Role.Issuer
                                             ))
    genesisTxs = genesis +: genesisPermitTxs
    masterToAlice = TransferTransactionV2
      .selfSigned(master, None, None, ts + 1, 3 * West, transferAssetWestFee, alice.toAddress, Array.emptyByteArray)
      .explicitGet()
    issue = IssueTransactionV2
      .selfSigned(currentChainId, alice, assetName, description, quantity, decimals, reissuable = false, West, ts + 100, None)
      .explicitGet()
    burn = BurnTransactionV2.selfSigned(currentChainId, alice, issue.assetId(), quantity / 2, West, ts + 200).explicitGet()
    reissue = ReissueTransactionV2
      .selfSigned(currentChainId, alice, issue.assetId(), burn.amount, reissuable = true, West, ts + 300)
      .explicitGet()
  } yield (ts, genesisTxs, masterToAlice, issue, burn, reissue)

  val localBlockchainSettings: BlockchainSettings = DefaultBlockchainSettings.copy(
    custom = DefaultBlockchainSettings.custom.copy(
      functionality = DefaultBlockchainSettings.custom.functionality.copy(
        featureCheckBlocksPeriod = 2,
        blocksForFeatureActivation = 1,
        preActivatedFeatures = Map(BlockchainFeature.NG.id -> 0, BlockchainFeature.DataTransaction.id -> 0, BlockchainFeature.SmartAccounts.id -> 0)
      )
    )
  )
  val localWESettings: WESettings = settings.copy(blockchain = localBlockchainSettings)

  property("issue -> burn -> reissue in sequential blocks works correctly") {
    scenario(preconditions, localWESettings) {
      case (domain, (ts, genesisTxs, masterToAlice, issue, burn, reissue)) =>
        val block0 = customBuildBlockOfTxs(randomSig, genesisTxs, defaultSigner, 1, ts)
        val block1 = customBuildBlockOfTxs(block0.uniqueId, Seq(masterToAlice), defaultSigner, 1, ts + 150)
        val block2 = customBuildBlockOfTxs(block1.uniqueId, Seq(issue), defaultSigner, 1, ts + 250)
        val block3 = customBuildBlockOfTxs(block2.uniqueId, Seq(burn), defaultSigner, 1, ts + 350)
        val block4 = customBuildBlockOfTxs(block3.uniqueId, Seq(reissue), defaultSigner, 1, ts + 450)

        domain.appendBlock(block0)
        domain.appendBlock(block1)

        domain.appendBlock(block2)
        val assetDescription1 = domain.blockchainUpdater.assetDescription(issue.assetId()).get
        assetDescription1.reissuable should be(false)
        assetDescription1.totalVolume should be(issue.quantity)

        domain.appendBlock(block3)
        val assetDescription2 = domain.blockchainUpdater.assetDescription(issue.assetId()).get
        assetDescription2.reissuable should be(false)
        assetDescription2.totalVolume should be(issue.quantity - burn.amount)

        domain.blockchainUpdater.processBlock(block4, ConsensusPostAction.NoAction) should produce("Asset is not reissuable")
    }
  }

  property("issue -> burn -> reissue in micro blocks works correctly") {
    scenario(preconditions, localWESettings) {
      case (domain, (ts, genesisTxs, masterToAlice, issue, burn, reissue)) =>
        val block0 = customBuildBlockOfTxs(randomSig, genesisTxs, defaultSigner, 1, ts)
        val block1 = customBuildBlockOfTxs(block0.uniqueId, Seq(masterToAlice), defaultSigner, 1, ts + 150)
        val block2 = customBuildBlockOfTxs(block1.uniqueId, Seq(issue), defaultSigner, 1, ts + 250)
        val block3 = customBuildBlockOfTxs(block2.uniqueId, Seq(burn, reissue), defaultSigner, 1, ts + 350)

        domain.appendBlock(block0)
        domain.appendBlock(block1)

        domain.appendBlock(block2)
        val assetDescription1 = domain.blockchainUpdater.assetDescription(issue.assetId()).get
        assetDescription1.reissuable should be(false)
        assetDescription1.totalVolume should be(issue.quantity)

        domain.blockchainUpdater.processBlock(block3, ConsensusPostAction.NoAction) should produce("Asset is not reissuable")
    }
  }
}
