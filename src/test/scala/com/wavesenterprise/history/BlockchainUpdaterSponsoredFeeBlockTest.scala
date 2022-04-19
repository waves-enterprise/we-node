package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.Role
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.settings.{BlockchainSettings, WESettings}
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.assets.{IssueTransaction, SponsorFeeTransactionV1}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BlockchainUpdaterSponsoredFeeBlockTest
    extends PropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  private val amtTx = 100000

  type Setup =
    (Seq[Transaction], TransferTransaction, IssueTransaction, SponsorFeeTransactionV1, TransferTransaction, TransferTransaction, TransferTransaction)

  val sponsorPreconditions: Gen[Setup] = for {

    master <- accountGen
    ts     <- timestampGen
    genesisTs = ts - 1000
    transferAssetWestFee        <- smallFeeGen
    alice                       <- accountGen
    bob                         <- accountGen
    (feeAsset, sponsorTx, _, _) <- sponsorFeeCancelSponsorFeeGen(alice, Some(ts))
    baseUnitRatio               = 1
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, genesisTs).explicitGet()
    genesisPermitTxs = buildGenesisPermitTxs(genesisTs,
                                             Map(
                                               defaultSigner.toAddress -> Role.Miner,
                                               alice.toAddress         -> Role.Issuer
                                             ))
    genesisTxs = genesis +: genesisPermitTxs

    masterToAlice = TransferTransactionV2
      .selfSigned(
        sender = master,
        assetId = None,
        feeAssetId = None,
        timestamp = ts + 1,
        amount = feeAsset.fee + sponsorTx.fee + transferAssetWestFee + transferAssetWestFee,
        fee = transferAssetWestFee,
        recipient = alice.toAddress,
        attachment = Array.emptyByteArray
      )
      .right
      .get
    aliceToBob = TransferTransactionV2
      .selfSigned(
        sender = alice,
        assetId = Some(feeAsset.id()),
        feeAssetId = None,
        timestamp = ts + 2,
        amount = feeAsset.quantity / 2,
        fee = transferAssetWestFee,
        recipient = bob.toAddress,
        attachment = Array.emptyByteArray
      )
      .right
      .get
    bobToMaster = TransferTransactionV2
      .selfSigned(
        sender = bob,
        assetId = Some(feeAsset.id()),
        feeAssetId = Some(feeAsset.id()),
        timestamp = ts + 3,
        amount = amtTx,
        fee = transferAssetWestFee * baseUnitRatio,
        recipient = master.toAddress,
        attachment = Array.emptyByteArray
      )
      .right
      .get
    bobToMaster2 = TransferTransactionV2
      .selfSigned(bob,
                  Some(feeAsset.id()),
                  Some(feeAsset.id()),
                  ts + 4,
                  amtTx,
                  transferAssetWestFee * baseUnitRatio,
                  master.toAddress,
                  Array.emptyByteArray)
      .right
      .get
  } yield (genesisTxs, masterToAlice, feeAsset, sponsorTx, aliceToBob, bobToMaster, bobToMaster2)

  val SponsoredFeeActivatedAt0BlockchainSettings: BlockchainSettings = DefaultBlockchainSettings.copy(
    custom = DefaultBlockchainSettings.custom
      .copy(
        functionality = DefaultBlockchainSettings.custom.functionality
          .copy(
            featureCheckBlocksPeriod = 2,
            blocksForFeatureActivation = 1,
            preActivatedFeatures = Map(BlockchainFeature.FeeSwitch.id            -> 0,
                                       BlockchainFeature.NG.id                   -> 0,
                                       BlockchainFeature.SmartAccounts.id        -> 0,
                                       BlockchainFeature.SponsoredFeesSupport.id -> 0)
          )
      )
  )

  val SponsoredActivatedAt0WESettings: WESettings = settings.copy(blockchain = SponsoredFeeActivatedAt0BlockchainSettings)

  property("not enough WEST to sponsor sponsored tx") {
    scenario(sponsorPreconditions, SponsoredActivatedAt0WESettings) {
      case (domain, (genesisTxs, masterToAlice, feeAsset, sponsor, aliceToBob, bobToMaster, bobToMaster2)) =>
        val (block0, microBlocks) = chainBaseAndMicro(randomSig, genesisTxs, Seq(masterToAlice, feeAsset, sponsor).map(Seq(_)))
        val block1                = customBuildBlockOfTxs(microBlocks.last.totalLiquidBlockSig, Seq.empty, accountGen.sample.get, 3: Byte, sponsor.timestamp + 1)
        val block2                = customBuildBlockOfTxs(block1.uniqueId, Seq.empty, accountGen.sample.get, 3: Byte, sponsor.timestamp + 1)
        val block3                = buildBlockOfTxs(block2.uniqueId, Seq(aliceToBob, bobToMaster))
        val block4                = buildBlockOfTxs(block3.uniqueId, Seq(bobToMaster2))

        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks(0)).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks(1)).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks(2)).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block2, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block3, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block4, ConsensusPostAction.NoAction) should produce("negative WEST balance" /*"unavailable funds"*/ )

    }
  }

}
