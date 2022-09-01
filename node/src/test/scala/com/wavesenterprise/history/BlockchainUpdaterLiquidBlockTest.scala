package com.wavesenterprise.history

import com.wavesenterprise._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{Block, TxMicroBlock}
import com.wavesenterprise.consensus.{ConsensusPostAction, PoSLikeConsensusBlockData}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.ENOUGH_AMT
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockchainUpdaterLiquidBlockTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  private def preconditionsAndPayments(minTx: Int, maxTx: Int): Gen[(Block, Block, Seq[TxMicroBlock])] =
    for {
      richAccount        <- accountGen
      totalTxNumber      <- Gen.chooseNum(minTx, maxTx)
      txNumberInKeyBlock <- Gen.chooseNum(0, Block.MaxTransactionsPerBlockVer3)
      allTxs             <- Gen.listOfN(totalTxNumber, validTransferGen(richAccount))
      prevBlockTs        <- ntpTimestampGen
      nextTs             <- Gen.chooseNum(100, 1000).map(_ + prevBlockTs)
    } yield {
      val (keyBlockTxs, microTxs) = allTxs.splitAt(txNumberInKeyBlock)
      val txNumberInMicros        = totalTxNumber - txNumberInKeyBlock

      val prevBlock = unsafeBlock(
        reference = randomSig,
        txs = Seq(GenesisTransaction.create(richAccount.toAddress, ENOUGH_AMT, 0).explicitGet()),
        signer = TestBlock.defaultSigner,
        version = 3,
        timestamp = prevBlockTs
      )

      val (keyBlock, microBlocks) = unsafeChainBaseAndMicro(
        totalRefTo = prevBlock.signerData.signature,
        base = keyBlockTxs,
        micros = microTxs.grouped(math.max(1, txNumberInMicros / 5)).toSeq,
        signer = TestBlock.defaultSigner,
        version = 3,
        timestamp = nextTs
      )

      (prevBlock, keyBlock, microBlocks)
    }

  property("liquid block can't be overfilled") {
    import Block.{MaxTransactionsPerBlockVer3 => Max}
    forAll(preconditionsAndPayments(Max + 1, Max + 100)) {
      case (prevBlock, keyBlock, microBlocks) =>
        withDomain(MicroblocksActivatedAt0WESettings) { d =>
          val blocksApplied = for {
            _ <- d.blockchainUpdater.processBlock(prevBlock, ConsensusPostAction.NoAction)
            _ <- d.blockchainUpdater.processBlock(keyBlock, ConsensusPostAction.NoAction)
          } yield ()

          val r = microBlocks.foldLeft(blocksApplied) {
            case (Right(_), curr) => d.blockchainUpdater.processMicroBlock(curr)
            case (x, _)           => x
          }

          withClue("All microblocks should not be processed") {
            r match {
              case Left(e: GenericError) => e.err should include("Limit of txs was reached")
              case x =>
                val txNumberByMicroBlock = microBlocks.map(_.transactionData.size)
                fail(
                  s"Unexpected result: $x. keyblock txs: ${keyBlock.blockHeader.transactionCount}, " +
                    s"microblock txs: ${txNumberByMicroBlock.mkString(", ")} (total: ${txNumberByMicroBlock.sum}), " +
                    s"total txs: ${keyBlock.blockHeader.transactionCount + txNumberByMicroBlock.sum}")
            }
          }
        }
    }
  }

  property("miner settings don't interfere with micro block processing") {
    val oneTxPerMicroSettings = MicroblocksActivatedAt0WESettings
      .copy(
        miner = MicroblocksActivatedAt0WESettings.miner.copy(
          maxTransactionsInMicroBlock = 1
        ))
    forAll(preconditionsAndPayments(10, Block.MaxTransactionsPerBlockVer3)) {
      case (genBlock, keyBlock, microBlocks) =>
        withDomain(oneTxPerMicroSettings) { d =>
          d.blockchainUpdater.processBlock(genBlock, ConsensusPostAction.NoAction)
          d.blockchainUpdater.processBlock(keyBlock, ConsensusPostAction.NoAction)
          microBlocks.foreach { mb =>
            d.blockchainUpdater.processMicroBlock(mb) shouldBe 'right
          }
        }
    }
  }

  private def validTransferGen(from: PrivateKeyAccount): Gen[Transaction] =
    for {
      amount    <- smallFeeGen
      feeAmount <- smallFeeGen
      timestamp <- ntpTimestampGen
      recipient <- accountGen
    } yield TransferTransactionV2.selfSigned(from, None, None, timestamp, amount, feeAmount, recipient.toAddress, Array.empty).explicitGet()

  private def unsafeChainBaseAndMicro(totalRefTo: ByteStr,
                                      base: Seq[Transaction],
                                      micros: Seq[Seq[Transaction]],
                                      signer: PrivateKeyAccount,
                                      version: Byte,
                                      timestamp: Long): (Block, Seq[TxMicroBlock]) = {
    val block = unsafeBlock(totalRefTo, base, signer, version, timestamp)
    val microBlocks = micros
      .foldLeft((block, Seq.empty[TxMicroBlock])) {
        case ((lastTotal, allMicros), txs) =>
          val (newTotal, micro) = unsafeMicro(totalRefTo, lastTotal, txs, signer, version, timestamp)
          (newTotal, allMicros :+ micro)
      }
      ._2
    (block, microBlocks)
  }

  private def unsafeMicro(totalRefTo: ByteStr,
                          prevTotal: Block,
                          txs: Seq[Transaction],
                          signer: PrivateKeyAccount,
                          version: Byte,
                          ts: Long): (Block, TxMicroBlock) = {
    val newTotalBlock = unsafeBlock(totalRefTo, prevTotal.transactionData ++ txs, signer, version, ts)
    val unsigned      = new TxMicroBlock(TxMicroBlock.DefaultVersion, signer, ts, txs, prevTotal.uniqueId, newTotalBlock.uniqueId, ByteStr.empty)
    val signature     = crypto.sign(signer, unsigned.bytes())
    val signed        = unsigned.copy(signature = ByteStr(signature))
    (newTotalBlock, signed)
  }

  private def unsafeBlock(reference: ByteStr,
                          txs: Seq[Transaction],
                          signer: PrivateKeyAccount,
                          version: Byte,
                          timestamp: Long,
                          bTarget: Long = DefaultBaseTarget): Block = {
    Block.buildAndSign(version, timestamp, reference, PoSLikeConsensusBlockData(bTarget, generationSignature), txs, signer, Set.empty).explicitGet()
  }

}
