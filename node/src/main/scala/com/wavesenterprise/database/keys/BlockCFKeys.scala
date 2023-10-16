package com.wavesenterprise.database.keys

import com.google.common.primitives.Ints
import com.wavesenterprise.block.BlockHeader
import com.wavesenterprise.database.KeyHelpers.{h, hash}
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.BlockCF
import com.wavesenterprise.database.{MainDBKey, readBlockHeaderAndSize, readTxIds, writeBlockHeaderAndSize, writeTxIds}
import com.wavesenterprise.state.ByteStr

object BlockCFKeys {

  val BlockScorePrefix: Short                = 1
  val BlockHeaderPrefix: Short               = 2
  val BlockHeightPrefix: Short               = 3
  val TransactionAtHeightPrefix: Short       = 4
  val BlockTransactionsAtHeightPrefix: Short = 5

  def score(height: Int): MainDBKey[BigInt] =
    MainDBKey("score", BlockCF, h(BlockScorePrefix, height), Option(_).fold(BigInt(0))(BigInt(_)), _.toByteArray)

  def blockHeaderAndSizeAt(height: Int): MainDBKey[Option[(BlockHeader, Int)]] =
    MainDBKey.opt("block-header-at-height", BlockCF, h(BlockHeaderPrefix, height), readBlockHeaderAndSize, writeBlockHeaderAndSize)

  def blockHeaderBytesAt(height: Int): MainDBKey[Option[Array[Byte]]] =
    MainDBKey.opt(
      "block-header-bytes-at-height",
      BlockCF,
      h(BlockHeaderPrefix, height),
      _.drop(4),
      _ => throw new Exception("Key \"block-header-bytes-at-height\" - is read only!")
    )

  def heightOf(blockId: ByteStr): MainDBKey[Option[Int]] =
    MainDBKey.opt("height-of", BlockCF, hash(BlockHeightPrefix, blockId), Ints.fromByteArray, Ints.toByteArray)

  def transactionIdsAtHeight(height: Int): MainDBKey[Seq[ByteStr]] =
    MainDBKey("transaction-ids-at-height", BlockCF, h(TransactionAtHeightPrefix, height), readTxIds, writeTxIds)

  def blockTransactionsAtHeight(height: Int): MainDBKey[Seq[ByteStr]] =
    MainDBKey("block-transaction-ids-at-height", BlockCF, h(BlockTransactionsAtHeightPrefix, height), readTxIds, writeTxIds)
}
