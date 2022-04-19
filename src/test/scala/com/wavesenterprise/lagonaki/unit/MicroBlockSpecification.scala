package com.wavesenterprise.lagonaki.unit

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{Block, MicroBlock, TxMicroBlock}
import com.wavesenterprise.crypto
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.mining.Miner
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.produce
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.transfer._
import org.scalamock.scalatest.MockFactory
import org.scalatest.words.ShouldVerb
import org.scalatest.{FunSuite, Matchers}

import scala.util.Random

class MicroBlockSpecification extends FunSuite with Matchers with MockFactory with ShouldVerb {

  val prevResBlockSig  = ByteStr(Array.fill(Block.BlockIdLength)(Random.nextInt(100).toByte))
  val totalResBlockSig = ByteStr(Array.fill(Block.BlockIdLength)(Random.nextInt(100).toByte))
  val reference        = Array.fill(Block.BlockIdLength)(Random.nextInt(100).toByte)
  val keyPairSender    = crypto.generateKeyPair()
  val sender           = PrivateKeyAccount(keyPairSender)
  val keyPairReference = crypto.generateKeyPair()
  val gen              = PrivateKeyAccount(keyPairReference).toAddress

  test("TxMicroBlock with txs bytes/parse roundtrip") {

    val ts      = System.currentTimeMillis() - 5000
    val tr      = TransferTransactionV2.selfSigned(sender, None, None, ts + 1, 5, 2, gen, Array()).explicitGet()
    val assetId = Some(ByteStr(Array.fill(AssetIdLength)(Random.nextInt(100).toByte)))
    val tr2     = TransferTransactionV2.selfSigned(sender, assetId, None, ts + 2, 5, 2, gen, Array()).explicitGet()

    val transactions = Seq(tr, tr2)

    val microBlock  = TxMicroBlock.buildAndSign(sender, ts + 3, transactions, prevResBlockSig, totalResBlockSig).explicitGet()
    val parsedBlock = MicroBlock.parseBytes(microBlock.bytes()).get.asInstanceOf[TxMicroBlock]

    assert(Signed.validate(microBlock).isRight)
    assert(Signed.validate(parsedBlock).isRight)

    assert(microBlock.signature == parsedBlock.signature)
    assert(microBlock.sender == parsedBlock.sender)
    assert(microBlock.totalLiquidBlockSig == parsedBlock.totalLiquidBlockSig)
    assert(microBlock.prevLiquidBlockSig == parsedBlock.prevLiquidBlockSig)
    assert(microBlock.transactionData == parsedBlock.transactionData)
    assert(microBlock == parsedBlock)
  }

  test("TxMicroBlock cannot be created with zero transactions") {

    val eitherBlockOrError = TxMicroBlock.buildAndSign(sender, 1L, Seq.empty[TransferTransaction], prevResBlockSig, totalResBlockSig)

    eitherBlockOrError should produce("cannot create MicroBlock with empty transactions")
  }

  test("TxMicroBlock cannot contain more than Miner.MaxTransactionsPerMicroblock") {

    val ts           = System.currentTimeMillis()
    val transaction  = TransferTransactionV2.selfSigned(sender, None, None, ts, 5, 1000, gen, Array()).explicitGet()
    val transactions = Seq.fill(Miner.MaxTransactionsPerMicroblock + 1)(transaction)

    val eitherBlockOrError = TxMicroBlock.buildAndSign(sender, ts + 1, transactions, prevResBlockSig, totalResBlockSig)

    eitherBlockOrError should produce("too many txs in MicroBlock")
  }
}
