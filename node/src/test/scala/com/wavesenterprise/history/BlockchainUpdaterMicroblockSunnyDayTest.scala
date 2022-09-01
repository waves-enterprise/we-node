package com.wavesenterprise.history

import com.wavesenterprise.account.{Address, AddressOrAlias, PrivateKeyAccount}
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{TransactionGen, crypto}
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockchainUpdaterMicroblockSunnyDayTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  type Setup = (GenesisTransaction, TransferTransactionV2, TransferTransactionV2, TransferTransactionV2)
  val preconditionsAndPayments: Gen[Setup] = for {
    master <- accountGen
    alice  <- accountGen
    bob    <- addressGen
    ts     <- ntpTimestampGen
    fee    <- smallFeeGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts - 5000).explicitGet()
    masterToAlice <- westTransferGeneratorP(master, alice.toAddress)
    aliceToBob  = createWestTransfer(alice, bob, masterToAlice.amount - fee - 1, fee, ts).explicitGet()
    aliceToBob2 = createWestTransfer(alice, bob, masterToAlice.amount - fee - 1, fee, ts + 1).explicitGet()
  } yield (genesis, masterToAlice, aliceToBob, aliceToBob2)

  property("all txs in different blocks: B0 <- B1 <- B2 <- B3!") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments) {
      case (domain, (genesis, masterToAlice, aliceToBob, aliceToBob2)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), Seq(masterToAlice), Seq(aliceToBob), Seq(aliceToBob2)))
        blocks.init.foreach(block => domain.blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction).explicitGet())
        domain.blockchainUpdater.processBlock(blocks.last, ConsensusPostAction.NoAction) should produce("unavailable funds")

        effBalance(genesis.recipient, domain) > 0 shouldBe true
        effBalance(masterToAlice.recipient, domain) shouldBe 0L
        effBalance(aliceToBob.recipient, domain) shouldBe 0L
    }
  }

  property("all txs in one block: B0 <- B0m1 <- B0m2 <- B0m3!") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, masterToAlice, aliceToBob, aliceToBob2)) =>
        val (block, microBlocks) = chainBaseAndMicro(randomSig, Seq(genesis), Seq(masterToAlice, aliceToBob, aliceToBob2).map(Seq(_)))
        domain.blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks.head).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks(1)).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks(2)) should produce("unavailable funds")
        domain.blockchainUpdater.lastBlock.get.transactionData shouldBe Seq(genesis, masterToAlice, aliceToBob)

        effBalance(genesis.recipient, domain) > 0 shouldBe true
        effBalance(masterToAlice.recipient, domain) > 0 shouldBe true
        effBalance(aliceToBob.recipient, domain) > 0 shouldBe true
    }
  }

  property("block references microBlock: B0 <- B1 <- B1m1 <- B2!") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, masterToAlice, aliceToBob, aliceToBob2)) =>
        val (block, microBlocks) = chainBaseAndMicro(randomSig, Seq(genesis), Seq(masterToAlice, aliceToBob, aliceToBob2).map(Seq(_)))
        domain.blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks.head).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks(1)).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks(2)) should produce("unavailable funds")

        effBalance(genesis.recipient, domain) > 0 shouldBe true
        effBalance(masterToAlice.recipient, domain) > 0 shouldBe true
        effBalance(aliceToBob.recipient, domain) > 0 shouldBe true
    }
  }

  property("discards some of microBlocks: B0 <- B0m1 <- B0m2; B0m1 <- B1") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, masterToAlice, aliceToBob, aliceToBob2)) =>
        val (block0, microBlocks0) = chainBaseAndMicro(randomSig, Seq(genesis), Seq(masterToAlice, aliceToBob).map(Seq(_)))
        val block1                 = buildBlockOfTxs(microBlocks0.head.totalLiquidBlockSig, Seq(aliceToBob2))
        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks0.head).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks0(1)).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction) shouldBe 'right

        effBalance(genesis.recipient, domain) > 0 shouldBe true
        effBalance(masterToAlice.recipient, domain) > 0 shouldBe true
        effBalance(aliceToBob.recipient, domain) shouldBe 0
    }
  }

  property("discards all microBlocks: B0 <- B1 <- B1m1; B1 <- B2") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, masterToAlice, aliceToBob, aliceToBob2)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microBlocks1) = chainBaseAndMicro(block0.uniqueId, Seq(masterToAlice), Seq(Seq(aliceToBob)))
        val block2                 = buildBlockOfTxs(block1.uniqueId, Seq(aliceToBob2))
        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks1.head).explicitGet()
        domain.blockchainUpdater.processBlock(block2, ConsensusPostAction.NoAction) shouldBe 'right

        effBalance(genesis.recipient, domain) > 0 shouldBe true
        effBalance(masterToAlice.recipient, domain) shouldBe 0
        effBalance(aliceToBob.recipient, domain) shouldBe 0
    }
  }

  property("doesn't discard liquid block if competitor is not better: B0 <- B1 <- B1m1; B0 <- B2!") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, masterToAlice, aliceToBob, aliceToBob2)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microBlocks1) = chainBaseAndMicro(block0.uniqueId, Seq(masterToAlice), Seq(Seq(aliceToBob)))
        val block2                 = buildBlockOfTxs(block0.uniqueId, Seq(aliceToBob2), block1.timestamp)
        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks1.head).explicitGet()
        domain.blockchainUpdater
          .processBlock(block2, ConsensusPostAction.NoAction)
          .explicitGet() // silently discards worse version

        effBalance(genesis.recipient, domain) > 0 shouldBe true
        effBalance(masterToAlice.recipient, domain) shouldBe 0
        effBalance(aliceToBob.recipient, domain) shouldBe 0
    }
  }

  property("discards liquid block if competitor is better: B0 <- B1 <- B1m1; B0 <- B2") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, masterToAlice, aliceToBob, aliceToBob2)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microBlocks1) = chainBaseAndMicro(block0.uniqueId, Seq(masterToAlice), Seq(Seq(aliceToBob)))
        val keyPair                = crypto.generateKeyPair()
        val otherSigner            = PrivateKeyAccount(keyPair)
        val block2 =
          customBuildBlockOfTxs(block0.uniqueId, Seq(masterToAlice, aliceToBob2), otherSigner, 1, masterToAlice.timestamp, DefaultBaseTarget / 2)
        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(microBlocks1.head).explicitGet()
        domain.blockchainUpdater.processBlock(block2, ConsensusPostAction.NoAction) shouldBe 'right

        effBalance(genesis.recipient, domain) > 0 shouldBe true
        effBalance(masterToAlice.recipient, domain) shouldBe 0
        effBalance(aliceToBob.recipient, domain) shouldBe 0
    }
  }

  property("discarding some of microBlocks doesn't affect resulting state") {
    forAll(preconditionsAndPayments, accountGen) {
      case ((genesis, masterToAlice, aliceToBob, aliceToBob2), miner) =>
        val ts = genesis.timestamp

        val minerABalance = withDomain(MicroblocksActivatedAt0WESettings) { da =>
          val block0a = customBuildBlockOfTxs(randomSig, Seq(genesis), miner, 3: Byte, ts)
          val (block1a, microBlocks1a) =
            chainBaseAndMicro(block0a.uniqueId, Seq(masterToAlice), Seq(MicroInfoForChain(Seq(aliceToBob), ts)), ts, signer = miner)
          val block2a = customBuildBlockOfTxs(block1a.uniqueId, Seq(aliceToBob2), miner, 3: Byte, ts)
          val block3a = customBuildBlockOfTxs(block2a.uniqueId, Seq.empty, miner, 3: Byte, ts)
          da.blockchainUpdater.processBlock(block0a, ConsensusPostAction.NoAction).explicitGet()
          da.blockchainUpdater.processBlock(block1a, ConsensusPostAction.NoAction).explicitGet()
          da.blockchainUpdater.processMicroBlock(microBlocks1a.head).explicitGet()
          da.blockchainUpdater.processBlock(block2a, ConsensusPostAction.NoAction).explicitGet()
          da.blockchainUpdater.processBlock(block3a, ConsensusPostAction.NoAction).explicitGet()

          da.portfolio(miner.toAddress).balance
        }

        val minerBBalance = withDomain(MicroblocksActivatedAt0WESettings) { db =>
          val block0b = customBuildBlockOfTxs(randomSig, Seq(genesis), miner, 3: Byte, ts)
          val block1b = customBuildBlockOfTxs(block0b.uniqueId, Seq(masterToAlice), miner, 3: Byte, ts)
          val block2b = customBuildBlockOfTxs(block1b.uniqueId, Seq(aliceToBob2), miner, 3: Byte, ts)
          val block3b = customBuildBlockOfTxs(block2b.uniqueId, Seq.empty, miner, 3: Byte, ts)
          db.blockchainUpdater.processBlock(block0b, ConsensusPostAction.NoAction).explicitGet()
          db.blockchainUpdater.processBlock(block1b, ConsensusPostAction.NoAction).explicitGet()
          db.blockchainUpdater.processBlock(block2b, ConsensusPostAction.NoAction).explicitGet()
          db.blockchainUpdater.processBlock(block3b, ConsensusPostAction.NoAction).explicitGet()

          db.portfolio(miner.toAddress).balance
        }

        minerABalance shouldBe minerBBalance
    }
  }

  private def effBalance(aa: AddressOrAlias, domain: Domain): Long = aa match {
    case address: Address => domain.effBalance(address)
    case _                => fail("Unexpected address object")
  }
}
