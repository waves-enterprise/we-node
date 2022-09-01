package com.wavesenterprise.transaction

import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.history.{BlockchainFactory, DefaultWESettings}
import com.wavesenterprise.settings.ConsensusType
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.docker.ContractTransactionGen
import com.wavesenterprise.transaction.smart.Verifier
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{TestHelpers, TestSchedulers, TestTime, TransactionGen, WithDB}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper.ExtendedGen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class AtomicTransactionV1Specification
    extends AnyPropSpec
    with CommonAtomicTransactionSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with TransactionGen
    with ContractTransactionGen
    with WithDB {

  private val signer = accountGen.generateSample()
  private val badge  = AtomicBadge(Some(signer.toAddress))

  property("Valid sender proof is verified") {
    val blockchain = buildBlockchain
    forAll(atomicTxV1Gen(signer, atomicSingleInnerTxGen(Some(badge)))) { tx =>
      Verifier(blockchain, blockchain.height)(tx) shouldBe 'right
    }
  }

  property("Invalid sender proof is unverified") {
    val blockchain = buildBlockchain
    forAll(atomicTxV1Gen(signer, atomicSingleInnerTxGen(Some(badge)))) { tx =>
      val proofArray = tx.proofs.proofs.head.arr
      proofArray.indices.foreach(proofArray.update(_, 0))
      Verifier(blockchain, blockchain.height)(tx) shouldBe 'left
    }
  }

  property("Valid miner proof is verified") {
    val blockchain = buildBlockchain
    forAll(atomicTxV1Gen(signer, atomicSingleInnerTxGen(Some(badge))), accountGen, accountGen, positiveLongGen) { (tx, senderAcc, minerAcc, ts) =>
      val minedTx = enrichAtomicTx(senderAcc, minerAcc, tx.transactions, tx, ts)
      Verifier(blockchain, blockchain.height)(minedTx) shouldBe 'right
    }
  }

  property("Invalid miner proof is unverified") {
    val blockchain = buildBlockchain
    forAll(atomicTxV1Gen(signer, atomicSingleInnerTxGen(Some(badge))), accountGen, accountGen, positiveLongGen) { (tx, senderAcc, minerAcc, ts) =>
      val minedTx    = enrichAtomicTx(senderAcc, minerAcc, tx.transactions, tx, ts)
      val proofArray = minedTx.proofs.proofs(Verifier.AtomicTxMinerProofIndex).arr
      proofArray.indices.foreach(proofArray.update(_, 0))
      Verifier(blockchain, blockchain.height)(minedTx) shouldBe 'left
    }
  }

  private def buildBlockchain: BlockchainUpdater with NG = {
    val genesisSigner          = accountGen.generateSample()
    val genesisSettings        = TestHelpers.buildGenesis(genesisSigner, Map.empty)
    val (_, blockchainUpdater) = BlockchainFactory(DefaultWESettings, storage, new TestTime(), TestSchedulers)
    blockchainUpdater
      .processBlock(Block.genesis(genesisSettings, ConsensusType.PoS).explicitGet(), ConsensusPostAction.NoAction)
      .explicitGet()
    blockchainUpdater
  }

}
