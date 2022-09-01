package com.wavesenterprise.database.keys

import com.google.common.primitives.Ints
import com.wavesenterprise.block.BlockHeader
import com.wavesenterprise.database.KeyHelpers.{h, hash}
import com.wavesenterprise.database.rocksdb.ColumnFamily.BlockCF
import com.wavesenterprise.database.{Key, readBlockHeaderAndSize, readTxIds, writeBlockHeaderAndSize, writeTxIds}
import com.wavesenterprise.state.ByteStr

object BlockCFKeys {

  val BlockScorePrefix: Short                = 1
  val BlockHeaderPrefix: Short               = 2
  val BlockHeightPrefix: Short               = 3
  val TransactionAtHeightPrefix: Short       = 4
  val BlockTransactionsAtHeightPrefix: Short = 5

  def score(height: Int): Key[BigInt] = Key("score", BlockCF, h(BlockScorePrefix, height), Option(_).fold(BigInt(0))(BigInt(_)), _.toByteArray)

  def blockHeaderAndSizeAt(height: Int): Key[Option[(BlockHeader, Int)]] =
    Key.opt("block-header-at-height", BlockCF, h(BlockHeaderPrefix, height), readBlockHeaderAndSize, writeBlockHeaderAndSize)

  def blockHeaderBytesAt(height: Int): Key[Option[Array[Byte]]] =
    Key.opt(
      "block-header-bytes-at-height",
      BlockCF,
      h(BlockHeaderPrefix, height),
      _.drop(4),
      _ => throw new Exception("Key \"block-header-bytes-at-height\" - is read only!")
    )

  def heightOf(blockId: ByteStr): Key[Option[Int]] =
    Key.opt("height-of", BlockCF, hash(BlockHeightPrefix, blockId), Ints.fromByteArray, Ints.toByteArray)

  def transactionIdsAtHeight(height: Int): Key[Seq[ByteStr]] =
    Key("transaction-ids-at-height", BlockCF, h(TransactionAtHeightPrefix, height), readTxIds, writeTxIds)

  def blockTransactionsAtHeight(height: Int): Key[Seq[ByteStr]] =
    Key("block-transaction-ids-at-height", BlockCF, h(BlockTransactionsAtHeightPrefix, height), readTxIds, writeTxIds)
}
