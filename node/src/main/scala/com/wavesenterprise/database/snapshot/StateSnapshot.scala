package com.wavesenterprise.database.snapshot

import java.nio.file.{Files, Path, Paths}

import cats.implicits._
import com.google.common.primitives.Shorts
import com.wavesenterprise.database.KeyHelpers.hBytes
import com.wavesenterprise.database.keys.{AddressCFKeys, ContractCFKeys, PermissionCFKeys, PrivacyCFKeys}
import com.wavesenterprise.database.rocksdb.ColumnFamily._
import com.wavesenterprise.database.rocksdb._
import com.wavesenterprise.database.snapshot.StateSnapshot.{GenesisHeight, KeyValue}
import com.wavesenterprise.database.{Keys, WEKeys, readIntSeq, writeIntSeq}
import com.wavesenterprise.utils.ResourceUtils.withResource
import com.wavesenterprise.utils.ScorexLogging
import org.apache.commons.io.FileUtils
import org.rocksdb.RocksIterator

import scala.util.Try

class StateSnapshot private (db: ReadOnlyDB, snapshotStorage: RocksDBOperations) extends ScorexLogging {

  import StateSnapshot._

  private[this] val simpleKeys = Seq(
    SimpleKeyMeta(Keys.AssetListPrefix),
    SimpleKeyMeta(Keys.AddressesForWestSeqPrefix),
    SimpleKeyMeta(Keys.AddressesForWestPrefix),
    SimpleKeyMeta(Keys.AddressesForAssetSeqPrefix),
    SimpleKeyMeta(Keys.AddressesForAssetPrefix),
    SimpleKeyMeta(Keys.AssetIdsPrefix),
    SimpleKeyMeta(Keys.DataKeysPrefix),
    SimpleKeyMeta(WEKeys.SetSizePrefix),
    SimpleKeyMeta(AddressCFKeys.AddressIdOfAliasPrefix, AddressCF),
    SimpleKeyMeta(AddressCFKeys.LastAddressIdPrefix, AddressCF),
    SimpleKeyMeta(AddressCFKeys.AddressIdPrefix, AddressCF),
    SimpleKeyMeta(AddressCFKeys.IdToAddressPrefix, AddressCF),
    SimpleKeyMeta(AddressCFKeys.LastNonEmptyRoleAddressPrefix, AddressCF),
    SimpleKeyMeta(AddressCFKeys.NonEmptyRoleAddressIdPrefix, AddressCF),
    SimpleKeyMeta(AddressCFKeys.NonEmptyRoleAddressPrefix, AddressCF),
    SimpleKeyMeta(WEKeys.SetSizePrefix, ContractCF),
    SimpleKeyMeta(ContractCFKeys.ContractIdsPrefix, ContractCF),
    SimpleKeyMeta(ContractCFKeys.ContractKeysPrefix, ContractCF),
    SimpleKeyMeta(PermissionCFKeys.PermissionPrefix, PermissionCF),
    SimpleKeyMeta(PermissionCFKeys.MinersKeysPrefix, PermissionCF),
    SimpleKeyMeta(PermissionCFKeys.ContractValidatorKeysPrefix, PermissionCF),
    SimpleKeyMeta(PermissionCFKeys.NetworkParticipantPrefix, PermissionCF),
    SimpleKeyMeta(PermissionCFKeys.NetworkParticipantSeqPrefix, PermissionCF),
    SimpleKeyMeta(WEKeys.SetSizePrefix, PrivacyCF),
    SimpleKeyMeta(PrivacyCFKeys.PolicyOwnersPrefix, PrivacyCF),
    SimpleKeyMeta(PrivacyCFKeys.PolicyRecipientsPrefix, PrivacyCF),
    SimpleKeyMeta(PrivacyCFKeys.PolicyDataHashPrefix, PrivacyCF),
    SimpleKeyMeta(PrivacyCFKeys.PolicyIdsPrefix, PrivacyCF)
  )

  private[this] val historyKeys = Seq(
    HistoryKeyMeta(Keys.WestBalanceHistoryPrefix, Keys.WestBalancePrefix),
    HistoryKeyMeta(Keys.AssetBalanceHistoryPrefix, Keys.AssetBalancePrefix),
    HistoryKeyMeta(Keys.AssetInfoHistoryPrefix, Keys.AssetInfoPrefix),
    HistoryKeyMeta(Keys.LeaseBalanceHistoryPrefix, Keys.LeaseBalancePrefix),
    HistoryKeyMeta(Keys.LeaseStatusHistoryPrefix, Keys.LeaseStatusPrefix),
    HistoryKeyMeta(Keys.FilledVolumeAndFeeHistoryPrefix, Keys.FilledVolumeAndFeePrefix),
    HistoryKeyMeta(Keys.AddressScriptHistoryPrefix, Keys.AddressScriptPrefix),
    HistoryKeyMeta(Keys.DataHistoryPrefix, Keys.DataPrefix),
    HistoryKeyMeta(Keys.SponsorshipHistoryPrefix, Keys.SponsorshipPrefix),
    HistoryKeyMeta(Keys.CarryFeeHistoryPrefix, Keys.CarryFeePrefix),
    HistoryKeyMeta(Keys.AssetScriptHistoryPrefix, Keys.AssetScriptPrefix),
    HistoryKeyMeta(ContractCFKeys.ContractHistoryPrefix, ContractCFKeys.ContractPrefix, ContractCF),
    HistoryKeyMeta(ContractCFKeys.ContractDataHistoryPrefix, ContractCFKeys.ContractDataPrefix, ContractCF)
  )

  def take(): Unit = {
    val height = db.get(Keys.height)
    log.info(s"Starting to take snapshot, blockchain height '$height'...")

    simpleKeys.foreach { kMeta =>
      log.debug(s"Processing keys with prefix '${kMeta.prefix}'...")
      val keyHandler = new SimpleKeyHandler(kMeta)
      iterateAndHandleKey(kMeta, keyHandler)
    }
    historyKeys.foreach { kMeta =>
      log.debug(s"Processing history keys with prefix '${kMeta.prefix}'...")
      val keyHandler = new HistoryKeyHandler(kMeta, db)
      iterateAndHandleKey(kMeta, keyHandler)
    }

    log.info("Snapshot was taken successfully")
  }

  private def iterateAndHandleKey[T <: KeyMeta](kMeta: T, keyHandler: KeyHandler[T]): Unit = {
    val columnFamily = kMeta.columnFamily
    val prefixBytes  = Shorts.toByteArray(kMeta.prefix)
    withResource(new BatchedRocksDBIterator(db, BatchKeysCount, columnFamily, prefixBytes)) { batchedIterator =>
      while (batchedIterator.hasNext) {
        withResource(batchedIterator.next()) { iterator =>
          snapshotStorage.readWrite { rw =>
            writeBatch(rw, iterator, keyHandler)
          }
        }
      }
    }
  }

  private def writeBatch(rw: RW, iterator: Iterator[KeyValue], handler: KeyHandler[_ <: KeyMeta]): Unit = {
    while (iterator.hasNext) {
      val (key, value) = iterator.next()
      handler.handle(rw, key, value)
    }
  }
}

/**
  * Key metadata description
  */
sealed trait KeyMeta {
  def prefix: Short
  def columnFamily: ColumnFamily
}

case class SimpleKeyMeta(prefix: Short, columnFamily: ColumnFamily = DefaultCF) extends KeyMeta

case class HistoryKeyMeta(prefix: Short, valueKeyPrefix: Short, columnFamily: ColumnFamily = DefaultCF) extends KeyMeta

/**
  * Handler for key values. Handles (key, value) tuples and writes them to snapshot storage
  */
sealed trait KeyHandler[T <: KeyMeta] {
  def keyMeta: T
  def handle(rw: RW, key: Array[Byte], value: Array[Byte]): Unit
}

class SimpleKeyHandler(val keyMeta: SimpleKeyMeta) extends KeyHandler[SimpleKeyMeta] {

  override def handle(rw: RW, key: Array[Byte], value: Array[Byte]): Unit = {
    rw.put(keyMeta.columnFamily, key, value)
  }
}

class HistoryKeyHandler(val keyMeta: HistoryKeyMeta, db: ReadOnlyDB) extends KeyHandler[HistoryKeyMeta] {

  override def handle(rw: RW, historyKey: Array[Byte], historyValue: Array[Byte]): Unit = {
    val columnFamily = keyMeta.columnFamily
    val history      = readIntSeq(historyValue)
    history.headOption.foreach { height =>
      val hkBytes = historyKey.drop(Keys.PrefixLength)
      val key     = hBytes(keyMeta.valueKeyPrefix, height, hkBytes)
      val value   = db.get(key, columnFamily)
      val newKey  = hBytes(keyMeta.valueKeyPrefix, GenesisHeight, hkBytes)
      rw.put(columnFamily, historyKey, writeIntSeq(Seq(GenesisHeight)))
      rw.put(columnFamily, newKey, value)
    }
  }
}

/**
  * Iterator over values for one key prefix.
  * Breaks iteration to smaller iterators of size $batchSize to minimize memory consumption by RocksDB.
  * We have to recreate iterators once a while, because an iterator will hold all the resources from being released.
  */
class BatchedRocksDBIterator(db: ReadOnlyDB, batchSize: Int, columnFamily: ColumnFamily, prefixBytes: Array[Byte])
    extends Iterator[Iterator[KeyValue] with AutoCloseable]
    with AutoCloseable {

  private var curr = newIterator(prefixBytes)

  private def newIterator(seek: Array[Byte]): RocksDBIteratorWrapper = {
    val iterator = db.iterator(columnFamily)
    iterator.seek(seek)
    new RocksDBIteratorWrapper(iterator)
  }

  override def hasNext: Boolean = (!curr.closed && curr.hasNext) || {
    curr.close()

    val last = curr.last
    last.exists {
      case (key, _) =>
        curr = newIterator(key)
        curr.iterator.next()
        curr.hasNext
    }
  }

  override def next(): Iterator[KeyValue] with AutoCloseable = {
    if (hasNext) {
      curr
    } else {
      Iterator.empty.next()
    }
  }

  override def close(): Unit = curr.close()

  class RocksDBIteratorWrapper(val iterator: RocksIterator) extends Iterator[KeyValue] with AutoCloseable {

    private var count                                            = 0
    protected[BatchedRocksDBIterator] var closed                 = false
    protected[BatchedRocksDBIterator] var last: Option[KeyValue] = None

    override def hasNext: Boolean = iterator.isValid && iterator.key().startsWith(prefixBytes) && count <= batchSize
    override def next(): KeyValue = {
      if (hasNext) {
        val result = (iterator.key(), iterator.value())
        last = Some(result)
        count += 1
        iterator.next()
        result
      } else {
        Iterator.empty.next()
      }
    }
    override def close(): Unit = {
      if (!closed) {
        iterator.close()
        closed = true
      }
    }
  }
}

object StateSnapshot {
  private val BatchKeysCount = 500
  private val MigratedFile   = "MIGRATED"

  protected[snapshot] val GenesisHeight = 1
  protected[snapshot] type KeyValue = (Array[Byte], Array[Byte])

  def exists(dir: String): Boolean = exists(Paths.get(dir))

  protected def exists(dir: Path): Boolean = {
    Files.exists(dir) && withResource(Files.list(dir)) { files =>
      files.filter(_.endsWith(MigratedFile)).findFirst().isPresent
    }
  }

  def create(db: ReadOnlyDB, dir: String): Either[Throwable, Unit] = {
    for {
      dirPath <- Either.catchNonFatal(Paths.get(dir))
      isEmpty <- Either.catchNonFatal {
        !Files.exists(dirPath) || withResource(Files.list(dirPath))(_.findAny().isEmpty)
      }
      _ <- Either.cond(isEmpty, (), new RuntimeException(s"Snapshot directory '$dir' is not empty"))
      _ <- Either
        .fromTry(Try {
          withResource(RocksDBStorage.openDB(dir, params = SnapshotParams)) { snapshotStorage =>
            new StateSnapshot(db, snapshotStorage).take()
          }
        })
        .leftMap { exception =>
          FileUtils.deleteDirectory(dirPath.toFile)
          exception
        }
      _ <- Either.catchNonFatal(Files.createFile(dirPath.resolve(MigratedFile)))
    } yield ()
  }
}
