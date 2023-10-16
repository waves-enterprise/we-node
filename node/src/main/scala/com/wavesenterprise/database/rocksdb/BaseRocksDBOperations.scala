package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.database.rocksdb.Listeners.listeners
import com.wavesenterprise.database.{BaseKey, KeyConstructors}
import com.wavesenterprise.utils.{NotEnoughDiskSpace, ScorexLogging, forceStopApplication}
import enumeratum.Enum
import org.apache.commons.io.FileUtils
import org.rocksdb.Status.Code.IOError
import org.rocksdb._

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

abstract class BaseRocksDBOperations[CF <: ColumnFamily, RO <: BaseReadOnlyDB[CF], RW <: BaseReadWriteDB[CF]] extends ScorexLogging
    with AutoCloseable {

  type Key[V] = BaseKey[V, CF]

  protected def db: RocksDB

  protected def columnHandles: Map[CF, ColumnFamilyHandle]

  protected def params: RocksDBParams

  protected def stats: Statistics

  protected def options: Options

  protected def databaseName: String

  protected def buildReadOnlyDb(db: RocksDB, readOptions: ReadOptions, columnHandles: Map[CF, ColumnFamilyHandle]): RO

  protected def buildReadWriteDb(db: RocksDB, readOptions: ReadOptions, batch: WriteBatch, columnHandles: Map[CF, ColumnFamilyHandle]): RW

  def presetCF: CF

  def keyConstructors: KeyConstructors[CF]

  def readOnly[A](f: RO => A): A = {
    val snapshot    = db.getSnapshot
    val readOptions = new ReadOptions().setSnapshot(snapshot)

    try {
      val readOnlyDB = buildReadOnlyDb(db, readOptions, columnHandles)
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
    val rw           = buildReadWriteDb(db, readOptions, batch, columnHandles)
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

  def newIterator(columnFamily: CF): RocksIterator = {
    val handle = columnHandles(columnFamily)
    db.newIterator(handle)
  }

  def notEnoughSpace[A](block: => A): A = {
    try {
      block
    } catch {
      case e: RocksDBException if e.getStatus.getCode == IOError && e.getMessage.toLowerCase.contains("no space left on device") =>
        log.error(s"Not enough space for $databaseName DB. Shutting down...")
        forceStopApplication(NotEnoughDiskSpace)
        throw e
    }
  }

  def closingActions(): Unit = {
    if (!params.readOnly) db.flush(new FlushOptions().setWaitForFlush(true), columnHandles.values.toList.asJava)
    columnHandles.values.foreach(_.close())
    db.close()
    options.close()
    stats.close()
  }

  private[this] val closed = new AtomicBoolean(false)

  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      closingActions()
    }
  }

}

object BaseRocksDBOperations {

  def prepareToCreateDB[CF <: ColumnFamily](
      path: String,
      params: RocksDBParams,
      columnFamily: Enum[CF],
      defaultColumnFamily: CF): (RocksDB, Map[CF, ColumnFamilyHandle], RocksDBParams, Statistics, Options) = {
    val file = new File(path)
    file.getParentFile.mkdirs()

    val stats = new Statistics

    val blockCache = new LRUCache(params.cacheSize)
    val tableConfig = new BlockBasedTableConfig()
      .setFilterPolicy(new BloomFilter(10, false))
      .setIndexType(IndexType.kHashSearch)
      .setBlockCache(blockCache)
      .setCacheIndexAndFilterBlocks(true)
      .setPinL0FilterAndIndexBlocksInCache(true)

    val options = new Options()
      .useCappedPrefixExtractor(java.lang.Short.BYTES)
      .setMemtablePrefixBloomSizeRatio(0.1)
      .setCompactionStyle(CompactionStyle.LEVEL)
      .setLevel0FileNumCompactionTrigger(10)
      .setMaxBytesForLevelBase(512 * FileUtils.ONE_MB)
      .setTableFormatConfig(tableConfig)
      .setCreateIfMissing(true)
      .setParanoidChecks(true)
      .setCreateMissingColumnFamilies(true)
      .setStatistics(stats)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setBytesPerSync(FileUtils.ONE_MB)
      .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
      .setLevelCompactionDynamicLevelBytes(true)
      .setMaxLogFileSize(10 * FileUtils.ONE_MB)
      .setLogFileTimeToRoll(Duration(7, TimeUnit.DAYS).toSeconds) // weekly rolling
      .setMaxWriteBufferNumber(params.maxWriteBufferNumber)
      .setAtomicFlush(params.atomicFlush)
      .setUnorderedWrite(params.unorderedWrite)
      .setListeners(listeners)

    val columnFamilyOptions = new ColumnFamilyOptions(options)

    val columnDescriptors =
      (if (params.onlyDefaultColumnFamily) Seq(defaultColumnFamily) else columnFamily.values)
        .map { cf =>
          new ColumnFamilyDescriptor(cf.entryName.getBytes(UTF_8), columnFamilyOptions)
        }

    val columnHandles = new ArrayBuffer[ColumnFamilyHandle]()

    val dbOptions = new DBOptions(options)

    val db = {
      if (params.readOnly)
        RocksDB.openReadOnly(dbOptions, path, columnDescriptors.asJava, columnHandles.asJava)
      else
        RocksDB.open(dbOptions, path, columnDescriptors.asJava, columnHandles.asJava)
    }

    val columnHandlesMap = columnHandles.map(ch => columnFamily.withName(new String(ch.getName, UTF_8)) -> ch).toMap

    (db, columnHandlesMap, params, stats, options)

  }
}
