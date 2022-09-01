package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.database.Key
import com.wavesenterprise.database.snapshot.StateSnapshot
import com.wavesenterprise.utils.{NotEnoughDiskSpace, ScorexLogging, forceStopApplication}
import org.rocksdb.Status.Code.IOError
import org.rocksdb._

trait RocksDBOperations {

  import RocksDBOperations._

  protected def db: RocksDB

  protected def columnHandles: Map[ColumnFamily, ColumnFamilyHandle]

  protected def params: RocksDBParams

  def readOnly[A](f: ReadOnlyDB => A): A = {
    val snapshot    = db.getSnapshot
    val readOptions = new ReadOptions().setSnapshot(snapshot)

    try {
      val readOnlyDB = new ReadOnlyDB(db, readOptions, columnHandles)
      f(readOnlyDB)
    } finally {
      db.releaseSnapshot(snapshot)
      readOptions.close()
      snapshot.close()
    }
  }

  def readWrite[A](block: RW => A): A = notEnoughSpace {
    val snapshot     = db.getSnapshot
    val writeOptions = new WriteOptions().setDisableWAL(params.disableWal)
    val readOptions  = new ReadOptions().setSnapshot(snapshot)
    val batch        = new WriteBatch()
    val rw           = new RW(db, readOptions, batch, columnHandles)
    try {
      val result = block(rw)
      db.write(writeOptions, batch)
      result
    } finally {
      db.releaseSnapshot(snapshot)
      batch.close()
      writeOptions.close()
      readOptions.close()
      snapshot.close()
    }
  }

  def get[A](key: Key[A]): A = {
    val handle = columnHandles(key.columnFamily)
    key.decode(db.get(handle, key.keyBytes))
  }

  def put[V](key: Key[V], value: V): Unit = {
    val handle = columnHandles(key.columnFamily)
    val bytes  = key.encode(value)
    db.put(handle, key.keyBytes, bytes)
  }

  def newIterator(): RocksIterator = db.newIterator()

  def newIterator(columnFamily: ColumnFamily): RocksIterator = {
    val handle = columnHandles(columnFamily)
    db.newIterator(handle)
  }

  def takeSnapshot(dir: String): Either[Throwable, Unit] = readOnly { db =>
    StateSnapshot.create(db, dir)
  }
}

object RocksDBOperations extends ScorexLogging {

  private def notEnoughSpace[A](block: => A): A = {
    try {
      block
    } catch {
      case e: RocksDBException if e.getStatus.getCode == IOError && e.getMessage.toLowerCase.contains("no space left on device") =>
        log.error("Not enough space for DB. Shutting down...")
        forceStopApplication(NotEnoughDiskSpace)
        throw e
    }
  }
}
