package com.wavesenterprise.database.rocksdb

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import com.wavesenterprise.database.migration.SchemaManager
import com.wavesenterprise.database.rocksdb.Listeners.listeners
import com.wavesenterprise.utils.ResourceUtils.closeQuietly
import com.wavesenterprise.utils.ScorexLogging
import org.apache.commons.io.FileUtils
import org.rocksdb._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class RocksDBStorage private (
    val db: RocksDB,
    val columnHandles: Map[ColumnFamily, ColumnFamilyHandle],
    val params: RocksDBParams,
    stats: Statistics,
    options: Options
) extends RocksDBOperations
    with AutoCloseable {

  private[this] val closed = new AtomicBoolean(false)

  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      if (!params.readOnly) db.flush(new FlushOptions().setWaitForFlush(true), columnHandles.values.toList.asJava)
      columnHandles.values.foreach(_.close())
      db.close()
      options.close()
      stats.close()
    }
  }
}

object RocksDBStorage extends ScorexLogging {

  RocksDB.loadLibrary()

  def openDB(path: String, migrateScheme: Boolean = true, params: RocksDBParams = DefaultParams): RocksDBStorage = {
    val storage = openRocksDB(path, params)
    if (migrateScheme) {
      try new SchemaManager(storage).migrateSchema()
      catch {
        case NonFatal(e) =>
          closeQuietly(storage)
          throw e
      }
    }
    storage
  }

  private def openRocksDB(path: String, params: RocksDBParams): RocksDBStorage = {
    log.debug(s"Open DB at '$path'")
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
      (if (params.onlyDefaultColumnFamily) Seq(ColumnFamily.DefaultCF) else ColumnFamily.values)
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

    val columnHandlesMap = columnHandles.map(ch => ColumnFamily.withName(new String(ch.getName, UTF_8)) -> ch).toMap

    new RocksDBStorage(db, columnHandlesMap, params, stats, options)
  }
}

sealed trait RocksDBParams {
  def cacheSize: Long
  def maxWriteBufferNumber: Int
  def disableWal: Boolean
  def atomicFlush: Boolean
  def unorderedWrite: Boolean

  def readOnly: Boolean                = false
  def onlyDefaultColumnFamily: Boolean = false
}

object DefaultParams extends RocksDBParams {
  override def cacheSize: Long           = 512 * FileUtils.ONE_MB
  override val maxWriteBufferNumber: Int = 2
  override val disableWal: Boolean       = false
  override val atomicFlush: Boolean      = false // isn't needed if WAL enabled
  override val unorderedWrite: Boolean   = false
}

object DefaultReadOnlyParams extends RocksDBParams {
  override def cacheSize: Long           = 512 * FileUtils.ONE_MB
  override val maxWriteBufferNumber: Int = 2
  override val disableWal: Boolean       = false
  override val atomicFlush: Boolean      = false // isn't needed if WAL enabled
  override val unorderedWrite: Boolean   = false
  override val readOnly: Boolean         = true
}

object SnapshotParams extends RocksDBParams {
  override def cacheSize: Long           = 8 * FileUtils.ONE_MB
  override val maxWriteBufferNumber: Int = 1
  override val disableWal: Boolean       = false
  override val atomicFlush: Boolean      = false
  override val unorderedWrite: Boolean   = false
}
