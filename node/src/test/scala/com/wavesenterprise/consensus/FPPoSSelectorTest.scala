package com.wavesenterprise.consensus

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.Block
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings._
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, ProduceError}
import com.wavesenterprise.transaction.{BlockchainUpdater, GenesisTransaction}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.{TestSchedulers, TransactionGen, WithDB}
import org.scalacheck.Gen
import org.scalatest.Ignore
import com.wavesenterprise.utils.EitherUtils.EitherExt

import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

@Ignore
class FPPoSSelectorTest extends AnyFreeSpec with Matchers with WithDB with TransactionGen {

  import FPPoSSelectorTest._

  implicit val _: Gen[PrivateKeyAccount] = accountGen

  "block delay" - {
    "same on the same height in different forks" in {
      withEnv(chainGen(List(ENOUGH_AMT / 2, ENOUGH_AMT / 3), 110)) {
        case Env(_, blockchain, miners) =>
          val miner1 = miners.head
          val miner2 = miners.tail.head

          val miner1Balance = blockchain.effectiveBalance(miner1.toAddress, blockchain.height, 0)

          val fork1 = mkFork(10, miner1, blockchain)
          val fork2 = mkFork(10, miner2, blockchain)

          val fork1Delay = {
            val blockForHit =
              fork1
                .lift(100)
                .orElse(blockchain.blockAt(blockchain.height + fork1.length - 100))
                .getOrElse(fork1.head)

            calcDelay(blockForHit, fork1.head.consensusData.asPoSMaybe().explicitGet().baseTarget, miner1.publicKey.getEncoded, miner1Balance)
          }

          val fork2Delay = {
            val blockForHit =
              fork2
                .lift(100)
                .orElse(blockchain.blockAt(blockchain.height + fork2.length - 100))
                .getOrElse(fork2.head)

            calcDelay(blockForHit, fork2.head.consensusData.asPoSMaybe().explicitGet().baseTarget, miner1.publicKey.getEncoded, miner1Balance)
          }

          fork1Delay shouldEqual fork2Delay
      }
    }
  }

  "block delay validation" - {
    "succeed when delay is correct" in {
      withEnv(chainGen(List(ENOUGH_AMT), 10)) {
        case Env(pos, blockchain, miners) =>
          val miner        = miners.head
          val height       = blockchain.height
          val minerBalance = blockchain.effectiveBalance(miner.toAddress, height, 0)
          val lastBlock    = blockchain.lastBlock.get
          val block        = forgeBlock(miner, blockchain, pos)()

          pos
            .validateBlockDelay(height + 1, block, lastBlock, minerBalance)
            .explicitGet()
      }
    }

    "failed when delay less than expected" in {
      withEnv(chainGen(List(ENOUGH_AMT), 10)) {
        case Env(pos, blockchain, miners) =>
          val miner        = miners.head
          val height       = blockchain.height
          val minerBalance = blockchain.effectiveBalance(miner.toAddress, height, 0)
          val lastBlock    = blockchain.lastBlock.get
          val block        = forgeBlock(miner, blockchain, pos)(updateDelay = _ - 1)

          pos
            .validateBlockDelay(
              height + 1,
              block,
              lastBlock,
              minerBalance
            ) should produce("less than min valid timestamp")
      }
    }
  }

  //TODO: this one is suspicious. Sometimes one of the cases fails, sometimes doesn't
  "base target validation" - {
    "succeed when BT is correct" in {
      withEnv(chainGen(List(ENOUGH_AMT), 10)) {
        case Env(pos, blockchain, miners) =>
          val miner     = miners.head
          val height    = blockchain.height
          val lastBlock = blockchain.lastBlock.get
          val block     = forgeBlock(miner, blockchain, pos)()

          pos
            .validateBaseTarget(
              height + 1,
              block,
              lastBlock,
              blockchain.blockAt(height - 2)
            ) shouldBe Right(())
      }
    }

    "failed when BT less than expected" in {
      withEnv(chainGen(List(ENOUGH_AMT), 10)) {
        case Env(pos, blockchain, miners) =>
          val miner     = miners.head
          val height    = blockchain.height
          val lastBlock = blockchain.lastBlock.get
          val block     = forgeBlock(miner, blockchain, pos)(updateBT = _ - 1)

          pos
            .validateBaseTarget(
              height + 1,
              block,
              lastBlock,
              blockchain.blockAt(height - 2)
            ) should produce("does not match calculated baseTarget")
      }
    }

    "failed when BT greater than expected" in {
      withEnv(chainGen(List(ENOUGH_AMT), 10)) {
        case Env(pos, blockchain, miners) =>
          val miner     = miners.head
          val height    = blockchain.height
          val lastBlock = blockchain.lastBlock.get
          val block     = forgeBlock(miner, blockchain, pos)(updateBT = _ + 1)

          pos
            .validateBaseTarget(
              height + 1,
              block,
              lastBlock,
              blockchain.blockAt(height - 2)
            ) should produce("does not match calculated baseTarget")
      }
    }
  }

  "generation signature validation" - {
    "succeed when GS is correct" in {
      withEnv(chainGen(List(ENOUGH_AMT), 10)) {
        case Env(pos, blockchain, miners) =>
          val miner  = miners.head
          val height = blockchain.height
          val block  = forgeBlock(miner, blockchain, pos)()

          pos
            .validateGeneratorSignature(
              height + 1,
              block
            ) shouldBe Right(())
      }
    }

    "failed when GS is incorrect" in {
      withEnv(chainGen(List(ENOUGH_AMT), 100)) {
        case Env(pos, blockchain, miners) =>
          val miner  = miners.head
          val height = blockchain.height
          val block  = forgeBlock(miner, blockchain, pos)(updateGS = gs => ByteStr(gs.arr |< Random.nextBytes))

          pos
            .validateGeneratorSignature(
              height + 1,
              block
            ) should produce("Generation signatures does not match")
      }
    }
  }

  "regression" - {
    "delay" in {
      PoSConsensus.calculateDelay(BigInt(1), 100l, 10000000000000l, 0) shouldBe 705491
      PoSConsensus.calculateDelay(BigInt(2), 200l, 20000000000000l, 0) shouldBe 607358
      PoSConsensus.calculateDelay(BigInt(3), 300l, 30000000000000l, 0) shouldBe 549956
    }

    "base target" in {
      PoSConsensus.calculateBaseTarget(30, 100l, 100000000000l, Some(99000l), 60, 100000l) shouldBe 99l
      PoSConsensus.calculateBaseTarget(10, 100l, 100000000000l, None, 60, 100000000000l) shouldBe 100l
      PoSConsensus.calculateBaseTarget(10, 100l, 100000000000l, Some(99999700000l), 60, 100000000000l) shouldBe 100l
      PoSConsensus.calculateBaseTarget(30, 100l, 100000000000l, Some(1l), 60, 1000000l) shouldBe 101l
    }
  }

  def withEnv(gen: Time => Gen[(Seq[PrivateKeyAccount], Seq[Block])])(f: Env => Unit): Unit = {
    val defaultWriter = new RocksDBWriter(storage, TestFunctionalitySettings.Stub, ConsensusSettings.PoSSettings, 100000, 2000)
    val settings0     = DefaultWESettings
    val settings      = settings0.copy(features = settings0.features.copy(autoShutdownOnUnsupportedFeature = false))
    val bcu           = new BlockchainUpdaterImpl(defaultWriter, settings, ntpTime, TestSchedulers)
    val pos           = new PoSConsensus(bcu, settings.blockchain)
    try {
      val (accounts, blocks) = gen(ntpTime).sample.get

      blocks.foreach { block =>
        bcu.processBlock(block, ConsensusPostAction.NoAction).explicitGet()
      }

      f(Env(pos, bcu, accounts))
      bcu.shutdown()
    } finally {
      bcu.shutdown()
    }
  }
}

object FPPoSSelectorTest {

  implicit class KComb[A](a: A) {
    def |<(f: A => Unit): A = {
      f(a)
      a
    }
  }

  final case class Env(pos: PoSConsensus, blockchain: BlockchainUpdater with NG, miners: Seq[PrivateKeyAccount])

  def produce(errorMessage: String): ProduceError = new ProduceError(errorMessage)

  def mkFork(blockCount: Int, miner: PrivateKeyAccount, blockchain: Blockchain): List[Block] = {
    val height = blockchain.height

    val lastBlock = blockchain.lastBlock.get

    ((1 to blockCount) foldLeft List(lastBlock)) { (forkChain, ind) =>
      val blockForHit =
        forkChain
          .lift(100)
          .orElse(blockchain.blockAt(height + ind - 100))
          .getOrElse(forkChain.head)

      val gs =
        PoSConsensus
          .generatorSignature(
            blockForHit.consensusData.asPoSMaybe().explicitGet().generationSignature.arr,
            miner.publicKey.getEncoded
          )

      val delay: Long = 60000

      val bt = PoSConsensus.calculateBaseTarget(
        60,
        height + ind - 1,
        forkChain.head.consensusData.asPoSMaybe().explicitGet().baseTarget,
        (forkChain.lift(2) orElse blockchain.blockAt(height + ind - 3)) map (_.timestamp),
        60,
        forkChain.head.timestamp + delay
      )

      val newBlock = Block
        .buildAndSign(
          3: Byte,
          forkChain.head.timestamp + delay,
          forkChain.head.uniqueId,
          PoSLikeConsensusBlockData(bt, ByteStr(gs)),
          Seq.empty,
          miner,
          Set.empty
        )
        .explicitGet()

      newBlock :: forkChain
    }
  }

  def forgeBlock(miner: PrivateKeyAccount, blockchain: Blockchain with NG, pos: PoSConsensus)(updateDelay: Long => Long = identity,
                                                                                              updateBT: Long => Long = identity,
                                                                                              updateGS: ByteStr => ByteStr = identity): Block = {
    val height       = blockchain.height
    val lastBlock    = blockchain.lastBlock.get
    val ggParentTS   = blockchain.blockAt(height - 2).map(_.timestamp)
    val minerBalance = blockchain.effectiveBalance(miner.toAddress, height, 0)
    val delay = updateDelay(
      pos
        .getValidBlockDelay(
          height,
          miner.publicKey.getEncoded,
          lastBlock.consensusData.asPoSMaybe().explicitGet().baseTarget,
          minerBalance
        )
        .explicitGet()
    )

    val cData = pos
      .consensusData(
        miner.publicKey.getEncoded,
        height,
        lastBlock.consensusData.asPoSMaybe().explicitGet().baseTarget,
        lastBlock.timestamp,
        ggParentTS,
        lastBlock.timestamp + delay
      )
      .explicitGet()

    val updatedCData = cData.copy(updateBT(cData.baseTarget), updateGS(cData.generationSignature))

    Block
      .buildAndSign(3: Byte, lastBlock.timestamp + delay, lastBlock.uniqueId, updatedCData, Seq.empty, miner, Set.empty)
      .explicitGet()
  }

  def chainGen(balances: List[Long], blockCount: Int)(t: Time)(
      implicit accountGen: Gen[PrivateKeyAccount]): Gen[(Seq[PrivateKeyAccount], Seq[Block])] = {
    val ts = t.correctedTime()

    Gen
      .listOfN(balances.length, accountGen)
      .map(_ zip balances)
      .map { accountsWithBalances =>
        for {
          (acc, balance) <- accountsWithBalances
          i = accountsWithBalances.indexOf((acc, balance))
        } yield (acc, GenesisTransaction.create(acc.toAddress, balance, ts + i).explicitGet())
      }
      .map { txs =>
        val lastTxTimestamp = txs.lastOption.map(_._2.timestamp) getOrElse ts
        val genesisBlock    = TestBlock.create(lastTxTimestamp + 1, txs.map(_._2))

        val chain = (1 to blockCount foldLeft List(genesisBlock)) { (blocks, d) =>
          val newBlock = TestBlock
            .create(
              lastTxTimestamp + 1 + d,
              blocks.head.uniqueId,
              Seq.empty
            )
          newBlock :: blocks
        }

        (txs.map(_._1), chain.reverse)
      }
  }

  def calcDelay(blockForHit: Block, prevBT: Long, minerPK: Array[Byte], effBalance: Long): Long = {

    val gs =
      PoSConsensus
        .generatorSignature(
          blockForHit.consensusData.asPoSMaybe().explicitGet().generationSignature.arr,
          minerPK
        )

    val hit = PoSConsensus.hit(gs)

    PoSConsensus.calculateDelay(hit, prevBT, effBalance, 0)
  }

}
