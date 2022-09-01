package com.wavesenterprise.lagonaki.mocks

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block._
import com.wavesenterprise.consensus.PoSLikeConsensusBlockData
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.utils.EitherUtils.EitherExt

import scala.util.{Random, Try}

object TestBlock {
  val keyPair = crypto.generateKeyPair()

  val defaultSigner = PrivateKeyAccount(keyPair)

  val random: Random = new Random()

  def randomOfLength(length: Int): ByteStr = ByteStr(Array.fill(length)(random.nextInt().toByte))

  def randomSignature(): ByteStr = randomOfLength(SignatureLength)

  def sign(signer: PrivateKeyAccount, b: Block): Block = {
    Block
      .buildAndSign(
        version = b.version,
        timestamp = b.timestamp,
        reference = b.reference,
        consensusData = b.consensusData,
        transactionData = b.transactionData,
        signer = signer,
        featureVotes = b.featureVotes
      )
      .explicitGet()
  }

  def create(txs: Seq[Transaction]): Block = create(defaultSigner, txs)

  def create(signer: PrivateKeyAccount, txs: Seq[Transaction]): Block =
    create(time = Try(txs.map(_.timestamp).max).getOrElse(0), txs = txs, signer = signer)

  def create(signer: PrivateKeyAccount, txs: Seq[Transaction], features: Set[Short]): Block =
    create(time = Try(txs.map(_.timestamp).max).getOrElse(0), ref = randomSignature(), txs = txs, signer = signer, version = 3, features = features)

  def create(time: Long, txs: Seq[Transaction]): Block = create(time, randomSignature(), txs, defaultSigner)

  def create(time: Long, txs: Seq[Transaction], signer: PrivateKeyAccount): Block = create(time, randomSignature(), txs, signer)

  def create(time: Long,
             ref: ByteStr,
             txs: Seq[Transaction],
             signer: PrivateKeyAccount = defaultSigner,
             version: Byte = 2,
             features: Set[Short] = Set.empty[Short]): Block =
    Block
      .buildAndSign(version,
                    time,
                    ref,
                    PoSLikeConsensusBlockData(2L, ByteStr(Array.fill(Block.GeneratorSignatureLength)(0: Byte))),
                    txs,
                    signer,
                    features)
      .explicitGet()

  def withReference(ref: ByteStr): Block =
    Block
      .buildAndSign(1, 0, ref, PoSLikeConsensusBlockData(2L, randomOfLength(Block.GeneratorSignatureLength)), Seq.empty, defaultSigner, Set.empty)
      .explicitGet()

  def withReferenceAndFeatures(ref: ByteStr, features: Set[Short]): Block =
    Block
      .buildAndSign(3, 0, ref, PoSLikeConsensusBlockData(2L, randomOfLength(Block.GeneratorSignatureLength)), Seq.empty, defaultSigner, features)
      .explicitGet()
}
