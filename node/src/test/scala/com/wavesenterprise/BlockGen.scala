package com.wavesenterprise

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.PoSLikeConsensusBlockData
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.{ProvenTransaction, Transaction}
import org.scalacheck.Gen
import org.scalatest.Suite

trait BlockGen extends TransactionGen { _: Suite =>

  import BlockGen._

  val blockParamGen: Gen[(Seq[ProvenTransaction], PrivateKeyAccount)] = for {
    count        <- Gen.choose(minTransactionsInBlockCount, maxTransactionsInBlockCount)
    transactions <- randomTransactionsGen(count)
    signer       <- accountGen
  } yield (transactions, signer)

  def versionedBlockGen(txs: Seq[Transaction], signer: PrivateKeyAccount, version: Byte): Gen[Block] =
    byteArrayGen(Block.BlockIdLength).flatMap(ref => versionedBlockGen(ByteStr(ref), txs, signer, version))

  def versionedBlockGen(reference: ByteStr, txs: Seq[Transaction], signer: PrivateKeyAccount, version: Byte): Gen[Block] =
    for {
      baseTarget          <- Gen.posNum[Long]
      generationSignature <- byteArrayGen(Block.GeneratorSignatureLength)
      timestamp           <- timestampGen
    } yield Block
      .buildAndSign(
        version,
        if (txs.isEmpty) timestamp else txs.map(_.timestamp).max,
        reference,
        PoSLikeConsensusBlockData(baseTarget, ByteStr(generationSignature)),
        txs,
        signer,
        Set.empty
      )
      .explicitGet()

  def blockGen(txs: Seq[Transaction], signer: PrivateKeyAccount): Gen[Block] = versionedBlockGen(txs, signer, 1)

  val randomSignerBlockGen: Gen[Block] = for {
    (transactions, signer) <- blockParamGen
    block                  <- blockGen(transactions, signer)
  } yield block

  val predefinedSignerBlockGen: Gen[Block] = for {
    (transactions, _) <- blockParamGen
    signer            <- Gen.const(predefinedSignerPrivateKey)
    block             <- blockGen(transactions, signer)
  } yield block

  val mixedBlockGen: Gen[Block] = for {
    block <- Gen.oneOf(randomSignerBlockGen, predefinedSignerBlockGen)
  } yield block

  def blocksSeqGen(blockGen: Gen[Block]): Gen[(Int, Int, Seq[Block])] =
    for {
      start      <- Gen.posNum[Int].label("from")
      end        <- Gen.chooseNum(start, start + 10).label("to")
      blockCount <- Gen.const(end - start + 1).label("actualBlockCount")
      blocks     <- Gen.listOfN(blockCount, blockGen).label("blocks")
    } yield (start, end, blocks)

  val randomBlocksSeqGen: Gen[(Int, Int, Seq[Block])] = blocksSeqGen(randomSignerBlockGen)

  val mixedBlocksSeqGen: Gen[(Int, Int, Seq[Block])] = blocksSeqGen(mixedBlockGen)

}

object BlockGen {
  val minTransactionsInBlockCount                   = 1
  val maxTransactionsInBlockCount                   = 10
  val keyPair                                       = crypto.generateKeyPair()
  val predefinedSignerPrivateKey: PrivateKeyAccount = PrivateKeyAccount(keyPair)
}
