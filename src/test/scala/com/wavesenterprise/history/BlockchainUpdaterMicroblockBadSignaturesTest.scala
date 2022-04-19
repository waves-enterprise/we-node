package com.wavesenterprise.history

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{TransactionGen, crypto}
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BlockchainUpdaterMicroblockBadSignaturesTest
    extends PropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  val preconditionsAndPayments: Gen[(GenesisTransaction, TransferTransactionV2, TransferTransactionV2)] = for {
    master    <- accountGen
    recipient <- accountGen
    ts        <- positiveIntGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    payment  <- westTransferGeneratorP(master, recipient.toAddress)
    payment2 <- westTransferGeneratorP(master, recipient.toAddress)
  } yield (genesis, payment, payment2)

  property("bad total resulting block signature") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments) {
      case (domain, (genesis, payment, payment2)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microblocks1) = chainBaseAndMicro(block0.uniqueId, Seq(payment), Seq(payment2).map(Seq(_)))
        val badSigMicro            = microblocks1.head.copy(totalLiquidBlockSig = randomSig)
        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(badSigMicro) should produce("InvalidSignature")
    }
  }

  property("bad microBlock signature") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments) {
      case (domain, (genesis, payment, payment2)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microblocks1) = chainBaseAndMicro(block0.uniqueId, Seq(payment), Seq(payment2).map(Seq(_)))
        val badSigMicro            = microblocks1.head.copy(signature = randomSig)
        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(badSigMicro) should produce("InvalidSignature")
    }
  }

  property("other sender") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments) {
      case (domain, (genesis, payment, payment2)) =>
        val keyPair     = crypto.generateKeyPair()
        val otherSigner = PrivateKeyAccount(keyPair)
        val block0      = buildBlockOfTxs(randomSig, Seq(genesis))
        val block1      = buildBlockOfTxs(block0.uniqueId, Seq(payment))
        val badSigMicro = buildMicroBlockOfTxs(block0.uniqueId, block1, Seq(payment2), otherSigner)._2
        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(badSigMicro) should produce("another account")
    }
  }
}
