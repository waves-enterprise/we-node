package com.wavesenterprise.database.rocksdb

import cats.effect.{IO, Resource}
import com.wavesenterprise.database.KeyConstructors
import com.wavesenterprise.database.migration.MainSchemaManager
import com.wavesenterprise.database.rocksdb.BaseRocksDBOperations.prepareToCreateDB
import com.wavesenterprise.database.snapshot.StateSnapshot
import com.wavesenterprise.utils.ResourceUtils.closeQuietly
import com.wavesenterprise.utils.ScorexLogging
import org.apache.commons.io.FileUtils
import org.rocksdb._

import scala.util.control.NonFatal

class MainRocksDBStorage(
    val db: RocksDB,
    val columnHandles: Map[MainDBColumnFamily, ColumnFamilyHandle],
    val params: RocksDBParams,
    val stats: Statistics,
    val options: Options
) extends BaseRocksDBOperations[MainDBColumnFamily, MainReadOnlyDB, MainReadWriteDB] {

  override val presetCF: MainDBColumnFamily = MainDBColumnFamily.PresetCF

  override def keyConstructors: KeyConstructors[MainDBColumnFamily] = com.wavesenterprise.database.MainDBKey
  override protected def databaseName: String                       = "main"

  override protected def buildReadOnlyDb(db: RocksDB,
                                         readOptions: ReadOptions,
                                         columnHandles: Map[MainDBColumnFamily, ColumnFamilyHandle]): MainReadOnlyDB =
    new MainReadOnlyDB(db, readOptions, columnHandles)

  override protected def buildReadWriteDb(db: RocksDB,
                                          readOptions: ReadOptions,
                                          batch: WriteBatch,
                                          columnHandles: Map[MainDBColumnFamily, ColumnFamilyHandle]): MainReadWriteDB =
    new MainReadWriteDB(db, readOptions, batch, columnHandles)

  def takeSnapshot(dir: String): Either[Throwable, Unit] = readOnly { db =>
    StateSnapshot.create(db, dir)
  }
}

object MainRocksDBStorage extends ScorexLogging {
  RocksDB.loadLibrary()

  def openDB(path: String, migrateScheme: Boolean = true, params: RocksDBParams = DefaultParams): MainRocksDBStorage = {
    log.debug(s"Open main DB at '$path'")
    val storage = openRocksDB(path, params)
    if (migrateScheme) {
      try new MainSchemaManager(storage).migrateSchema()
      catch {
        case NonFatal(e) =>
          closeQuietly(storage)
          throw e
      }
    }
    storage
  }

  private def openRocksDB(path: String, params: RocksDBParams): MainRocksDBStorage = {

    val (db, columnHandlesMap, parameters, stats, options) = prepareToCreateDB(path, params, MainDBColumnFamily, MainDBColumnFamily.PresetCF)

    new MainRocksDBStorage(db, columnHandlesMap, parameters, stats, options)
  }

  def withRocksDB[T](path: String, params: RocksDBParams = DefaultParams)(f: MainRocksDBStorage => IO[T]): IO[T] = {
    Resource.fromAutoCloseable(IO(MainRocksDBStorage.openRocksDB(path, params))).use(f)
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
