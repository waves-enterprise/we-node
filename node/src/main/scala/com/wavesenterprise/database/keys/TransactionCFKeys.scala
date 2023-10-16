package com.wavesenterprise.database.keys

import com.wavesenterprise.database.KeyHelpers.hash
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.TransactionCF
import com.wavesenterprise.database.{MainDBKey, readTransactionInfo, writeTransactionInfo}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.Transaction

object TransactionCFKeys {

  val TransactionInfoPrefix: Short = 1

  def transactionInfo(txId: ByteStr): MainDBKey[Option[(Int, Transaction)]] =
    MainDBKey.opt("transaction-info", TransactionCF, hash(TransactionInfoPrefix, txId), readTransactionInfo, writeTransactionInfo)

  def transactionBytes(txId: ByteStr): MainDBKey[Option[Array[Byte]]] =
    MainDBKey.opt(
      "transaction-info-bytes",
      TransactionCF,
      hash(TransactionInfoPrefix, txId),
      _.drop(4),
      _ => throw new RuntimeException("Key \"transaction-info-bytes\" - is read only!")
    )
}
