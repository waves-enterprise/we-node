package com.wavesenterprise.database.rocksdb.confidential

import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.database.KeyHelpers.bytes
import com.wavesenterprise.database.rocksdb.ConfidentialDBColumnFamily
import com.wavesenterprise.database.{readIntSeq, writeIntSeq}

object ConfidentialKeyHelpers {
  def historyKey(name: String, columnFamily: ConfidentialDBColumnFamily, prefix: Short, b: Array[Byte]): ConfidentialKey[Seq[Int]] =
    ConfidentialDBKey(name, columnFamily, bytes(prefix, b), readIntSeq, writeIntSeq)

  def intKey(name: String, prefix: Short, default: Int = 0): ConfidentialKey[Int] =
    ConfidentialDBKey(name, Shorts.toByteArray(prefix), Option(_).fold(default)(Ints.fromByteArray), Ints.toByteArray)
}
