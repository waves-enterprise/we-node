package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.database.BaseKey

trait HistoryUpdater[CF <: ColumnFamily, RO <: BaseReadOnlyDB[CF], RW <: BaseReadWriteDB[CF]] {

  type Key[V] = BaseKey[V, CF]

  protected def updateHistory(rw: RW, key: Key[Seq[Int]], threshold: Int, kf: Int => Key[_])(height: Int): (CF, Seq[Array[Byte]]) =
    updateHistory(rw, rw.get(key), key, threshold, kf)(height)

  protected def updateHistory(rw: RW,
                              history: Seq[Int],
                              key: Key[Seq[Int]],
                              threshold: Int,
                              kf: Int => Key[_])(height: Int): (CF, Seq[Array[Byte]]) = {
    val (c1, c2) = history.partition(_ > threshold)
    rw.put(key, (height +: c1) ++ c2.headOption)
    (key.columnFamily, c2.drop(1).map(kf(_).keyBytes))
  }
}
