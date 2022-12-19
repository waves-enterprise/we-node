package com.wavesenterprise.history

import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{Block, MicroBlock}
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.transfer._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockchainUpdaterBlockMicroblockSequencesSameTransactionsTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen
    with NoShrink {

  import BlockchainUpdaterBlockMicroblockSequencesSameTransactionsTest._

  property("resulting miner balance should not depend on tx distribution among blocks and microblocks") {
    forAll(g(100, 5)) {
      case (gen, rest) =>
        val finalMinerBalances = rest.map {
          case (a @ (bmb: BlockAndMicroblockSequence, last: Block)) =>
            withDomain(MicroblocksActivatedAt0WESettings) { d =>
              d.blockchainUpdater.processBlock(gen, ConsensusPostAction.NoAction).explicitGet()
              bmb.foreach {
                case (b, mbs) =>
                  d.blockchainUpdater.processBlock(b, ConsensusPostAction.NoAction).explicitGet()
                  mbs.foreach(mb => d.blockchainUpdater.processMicroBlock(mb).explicitGet())
              }
              d.blockchainUpdater.processBlock(last, ConsensusPostAction.NoAction)
              d.portfolio(last.signerData.generator.toAddress).balance
            }
        }
        finalMinerBalances.toSet.size shouldBe 1
    }
  }

  property("Miner fee from microblock [Genesis] <- [Empty] <~ (Micro with tx) <- [Empty]") {
    val preconditionsAndPayments: Gen[(PrivateKeyAccount, GenesisTransaction, TransferTransactionV2, Int)] = for {
      master <- accountGen
      miner  <- accountGen
      ts     <- positiveIntGen
      fee    <- smallFeeGen
      amt    <- smallFeeGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      payment                     = createWestTransfer(master, master.toAddress, amt, fee, ts).explicitGet()
    } yield (miner, genesis, payment, ts)
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (miner, genesis, payment, ts)) =>
        val genBlock       = buildBlockOfTxs(randomSig, Seq(genesis))
        val (base, micros) = chainBaseAndMicro(genBlock.uniqueId, Seq.empty, Seq(MicroInfoForChain(Seq(payment), ts)), ts, miner)
        val emptyBlock     = customBuildBlockOfTxs(micros.last.totalLiquidBlockSig, Seq.empty, miner, 3, ts)
        domain.blockchainUpdater.processBlock(genBlock, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(base, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processMicroBlock(micros.head).explicitGet()
        domain.blockchainUpdater.processBlock(emptyBlock, ConsensusPostAction.NoAction).explicitGet()

        domain.portfolio(miner.toAddress).balance shouldBe payment.fee
        domain.portfolio(genesis.recipient).balance shouldBe (genesis.amount - payment.fee)
    }
  }

  def randomPayment(accs: Seq[PrivateKeyAccount], ts: Long): Gen[TransferTransactionV2] =
    for {
      from <- Gen.oneOf(accs)
      to   <- Gen.oneOf(accs)
      fee  <- smallFeeGen
      amt  <- smallFeeGen
    } yield createWestTransfer(from, to.toAddress, amt, fee, ts).explicitGet()

  def randomPayments(accs: Seq[PrivateKeyAccount], ts: Long, amt: Int): Gen[Seq[TransferTransactionV2]] =
    if (amt == 0)
      Gen.const(Seq.empty)
    else
      for {
        h <- randomPayment(accs, ts)
        t <- randomPayments(accs, ts + 1, amt - 1)
      } yield h +: t

  val TOTAL_WEST: Long = ENOUGH_AMT

  def accsAndGenesis(): Gen[(Seq[PrivateKeyAccount], PrivateKeyAccount, Block, Int)] =
    for {
      alice   <- accountGen
      bob     <- accountGen
      charlie <- accountGen
      dave    <- accountGen
      miner   <- accountGen
      ts      <- positiveIntGen
      genesis1: GenesisTransaction = GenesisTransaction.create(alice.toAddress, TOTAL_WEST / 4, ts).explicitGet()
      genesis2: GenesisTransaction = GenesisTransaction.create(bob.toAddress, TOTAL_WEST / 4, ts + 1).explicitGet()
      genesis3: GenesisTransaction = GenesisTransaction.create(charlie.toAddress, TOTAL_WEST / 4, ts + 2).explicitGet()
      genesis4: GenesisTransaction = GenesisTransaction.create(dave.toAddress, TOTAL_WEST / 4, ts + 4).explicitGet()
    } yield (Seq(alice, bob, charlie, dave),
             miner,
             customBuildBlockOfTxs(randomSig, Seq(genesis1, genesis2, genesis3, genesis4), defaultSigner, 1, ts),
             ts)

  def g(totalTxs: Int, totalScenarios: Int): Gen[(Block, Seq[(BlockAndMicroblockSequence, Block)])] =
    for {
      aaa @ (accs, miner, genesis, ts)      <- accsAndGenesis()
      payments                              <- randomPayments(accs, ts, totalTxs)
      intSeqs: Seq[BlockAndMicroblockSizes] <- randomSequences(totalTxs, totalScenarios)
    } yield {
      val blocksAndMicros = intSeqs.map { intSeq =>
        val blockAndMicroblockSequence = r(payments, intSeq, genesis.uniqueId, miner, ts)
        val ref                        = bestRef(blockAndMicroblockSequence.last)
        val lastBlock                  = customBuildBlockOfTxs(ref, Seq.empty, miner, Block.NgBlockVersion, ts)
        (blockAndMicroblockSequence, lastBlock)
      }
      (genesis, blocksAndMicros)
    }
}

object BlockchainUpdaterBlockMicroblockSequencesSameTransactionsTest {

  def genSizes(total: Int): Gen[Seq[Int]] =
    for {
      h <- Gen.choose(1, total)
      t <- if (h < total) genSizes(total - h) else Gen.const(Seq.empty)
    } yield h +: t

  def genSplitSizes(total: Int): Gen[(Int, Seq[Int])] = genSizes(total).map { case (h :: tail) => (h, tail) }

  type BlockAndMicroblockSize     = (Int, Seq[Int])
  type BlockAndMicroblockSizes    = Seq[BlockAndMicroblockSize]
  type BlockAndMicroblocks        = (Block, Seq[MicroBlock])
  type BlockAndMicroblockSequence = Seq[BlockAndMicroblocks]

  def randomSizeSequence(total: Int): Gen[BlockAndMicroblockSizes] =
    for {
      totalStep <- Gen.choose(1, Math.min(Math.min(total / 3 + 2, total), 250))
      h         <- genSplitSizes(totalStep)
      t         <- if (totalStep < total) randomSizeSequence(total - totalStep) else Gen.const(Seq.empty)
    } yield h +: t

  def randomSequences(total: Int, sequences: Int): Gen[Seq[BlockAndMicroblockSizes]] =
    if (sequences == 0)
      Gen.const(Seq.empty)
    else
      for {
        h <- randomSizeSequence(total)
        t <- randomSequences(total, sequences - 1)
      } yield h +: t

  def take(txs: Seq[Transaction], sizes: BlockAndMicroblockSize): ((Seq[Transaction], Seq[Seq[Transaction]]), Seq[Transaction]) = {
    val (blockAmt, microsAmts) = sizes
    val (blockTxs, rest)       = txs.splitAt(blockAmt)
    val (reversedMicroblockTxs, res) = microsAmts.foldLeft((Seq.empty[Seq[Transaction]], rest)) {
      case ((acc, pool), amt) =>
        val (step, next) = pool.splitAt(amt)
        (step +: acc, next)
    }
    ((blockTxs, reversedMicroblockTxs.reverse), res)
  }

  def stepR(txs: Seq[Transaction],
            sizes: BlockAndMicroblockSize,
            prev: ByteStr,
            signer: PrivateKeyAccount,
            timestamp: Long): (BlockAndMicroblocks, Seq[Transaction]) = {
    val ((blockTxs, microblockTxs), rest) = take(txs, sizes)
    (chainBaseAndMicro(prev, blockTxs, microblockTxs.map(txs => MicroInfoForChain(txs, timestamp)), timestamp, signer), rest)
  }

  def bestRef(r: BlockAndMicroblocks): ByteStr = r._2.lastOption match {
    case Some(mb) => mb.totalLiquidBlockSig
    case None     => r._1.uniqueId
  }

  def r(txs: Seq[Transaction],
        sizes: BlockAndMicroblockSizes,
        initial: ByteStr,
        signer: PrivateKeyAccount,
        timestamp: Long): BlockAndMicroblockSequence = {
    sizes
      .foldLeft((Seq.empty[BlockAndMicroblocks], txs)) {
        case ((acc, rest), s) =>
          val prev         = acc.headOption.map(bestRef).getOrElse(initial)
          val (step, next) = stepR(rest, s, prev, signer, timestamp)
          (step +: acc, next)
      }
      ._1
      .reverse
  }
}
