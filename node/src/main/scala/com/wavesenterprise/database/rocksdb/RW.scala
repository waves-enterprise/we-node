package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.database.Key
import com.wavesenterprise.metrics.DBStats
import com.wavesenterprise.metrics.DBStats.DbHistogramExt
import org.rocksdb.{ColumnFamilyHandle, ReadOptions, RocksDB, WriteBatch}

class RW(db: RocksDB, readOptions: ReadOptions, batch: WriteBatch, columnHandles: Map[ColumnFamily, ColumnFamilyHandle])
    extends ReadOnlyDB(db, readOptions, columnHandles) {

  def put[V](key: Key[V], value: V): Unit = {
    val handle = columnHandles(key.columnFamily)
    val bytes  = key.encode(value)
    DBStats.write.recordTagged(key, bytes)
    batch.put(handle, key.keyBytes, bytes)
  }

  def update[V](key: Key[V])(f: V => V): Unit = put(key, f(get(key)))

  def delete(columnFamily: ColumnFamily, keys: Seq[Array[Byte]]): Unit = {
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

  protected[database] def put(columnFamily: ColumnFamily, keyBytes: Array[Byte], valueBytes: Array[Byte]): Unit = {
    val handle = columnHandles(columnFamily)
    batch.put(handle, keyBytes, valueBytes)
  }
}
