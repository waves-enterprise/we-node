package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.metrics.DBStats
import com.wavesenterprise.metrics.DBStats.DbHistogramExt
import org.rocksdb.{ColumnFamilyHandle, ReadOptions, RocksDB, WriteBatch}

trait BaseReadWriteDB[CF <: ColumnFamily]
    extends BaseReadOnlyDB[CF] {
  def batch: WriteBatch

  def put[V](key: Key[V], value: V): Unit = {
    val handle = columnHandles(key.columnFamily)
    val bytes  = key.encode(value)
    DBStats.write.recordTagged(key, bytes)
    batch.put(handle, key.keyBytes, bytes)
  }

  def update[V](key: Key[V])(f: V => V): Unit = put(key, f(get(key)))

  def delete(columnFamily: CF, keys: Seq[Array[Byte]]): Unit = {
    val handle = columnHandles(columnFamily)
    keys.foreach(key => batch.delete(handle, key))
  }

  def delete[V](key: Key[V]): Unit = {
    val handle = columnHandles(key.columnFamily)
    batch.delete(handle, key.keyBytes)
  }

  def filterHistory(key: Key[Seq[Int]], heightToRemove: Int): Seq[Int] = {
    val history  = get(key)
    val filtered = history.filterNot(_ == heightToRemove)
    put(key, filtered)
    filtered
  }

  protected[database] def put(columnFamily: CF, keyBytes: Array[Byte], valueBytes: Array[Byte]): Unit = {
    val handle = columnHandles(columnFamily)
    batch.put(handle, keyBytes, valueBytes)
  }
}

class MainReadWriteDB(db: RocksDB, readOptions: ReadOptions, val batch: WriteBatch, columnHandles: Map[MainDBColumnFamily, ColumnFamilyHandle])
    extends MainReadOnlyDB(db, readOptions, columnHandles) with BaseReadWriteDB[MainDBColumnFamily]
