package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.database.Key
import com.wavesenterprise.metrics.DBStats
import com.wavesenterprise.metrics.DBStats.DbHistogramExt
import org.rocksdb.{ColumnFamilyHandle, ReadOptions, RocksDB, RocksIterator}
import DBStats.TimerExt

class ReadOnlyDB(db: RocksDB, readOptions: ReadOptions, columnHandles: Map[ColumnFamily, ColumnFamilyHandle]) {

  def get[V](key: Key[V]): V = {
    val handle = columnHandles(key.columnFamily)

    val bytes = DBStats.readLatency.measure(key) {
      db.get(handle, readOptions, key.keyBytes)
    }

    DBStats.read.recordTagged(key, bytes)
    key.decode(bytes)
  }

  def has[V](key: Key[V]): Boolean = {
    val handle = columnHandles(key.columnFamily)

    val keyMayExist = DBStats.existLatency.measure(key) {
      db.keyMayExist(handle, readOptions, key.keyBytes, null)
    }

    if (keyMayExist) {
      val bytes = DBStats.existNotDefinitelyLatency.measure(key) {
        db.get(handle, readOptions, key.keyBytes)
      }

      DBStats.read.recordTagged(key, bytes)
      bytes != null
    } else {
      false
    }
  }

  def iterator: RocksIterator = db.newIterator(readOptions)

  def iterator(columnFamily: ColumnFamily): RocksIterator = {
    val handle = columnHandles(columnFamily)
    db.newIterator(handle, readOptions)
  }

  protected[database] def get(keyBytes: Array[Byte], columnFamily: ColumnFamily): Array[Byte] = {
    val handle = columnHandles(columnFamily)
    db.get(handle, readOptions, keyBytes)
  }
}
