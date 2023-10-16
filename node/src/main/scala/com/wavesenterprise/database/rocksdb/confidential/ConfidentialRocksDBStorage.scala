package com.wavesenterprise.database.rocksdb.confidential

import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.database.KeyConstructors
import com.wavesenterprise.database.RocksDBSet._
import com.wavesenterprise.database.rocksdb.BaseRocksDBOperations.prepareToCreateDB
import com.wavesenterprise.database.rocksdb.confidential.migration.ConfidentialSchemaManager
import com.wavesenterprise.database.rocksdb._
import com.wavesenterprise.network.contracts.{ConfidentialContractDataId, ConfidentialDataId, ConfidentialDataType}
import com.wavesenterprise.state.ContractId
import com.wavesenterprise.state.contracts.confidential.{ConfidentialInput, ConfidentialOutput}
import com.wavesenterprise.utils.ResourceUtils.closeQuietly
import com.wavesenterprise.utils.ScorexLogging
import org.rocksdb._

import scala.util.control.NonFatal

class ConfidentialRocksDBStorage private (
    val db: RocksDB,
    val columnHandles: Map[ConfidentialDBColumnFamily, ColumnFamilyHandle],
    val params: RocksDBParams,
    val stats: Statistics,
    val options: Options
) extends BaseRocksDBOperations[ConfidentialDBColumnFamily, ConfidentialReadOnlyDB, ConfidentialReadWriteDB] {

  override def presetCF: ConfidentialDBColumnFamily = ConfidentialDBColumnFamily.PresetCF

  override def keyConstructors: KeyConstructors[ConfidentialDBColumnFamily] = ConfidentialDBKey

  override protected def databaseName: String = "confidential"

  def saveInput(input: ConfidentialInput): Unit = {
    readWrite { rw =>
      rw.put(ConfidentialInputCFKeys.confidentialInputByCommitment(input.commitment), Some(input))
    }
  }

  def saveOutput(output: ConfidentialOutput): Unit = {
    readWrite { rw =>
      rw.put(ConfidentialOutputCFKeys.confidentialOutputByCommitment(output.commitment), Some(output))
    }
  }

  def getInput(commitment: Commitment): Option[ConfidentialInput] = readOnly { rw =>
    rw.get(ConfidentialInputCFKeys.confidentialInputByCommitment(commitment))
  }

  def inputExists(commitment: Commitment): Boolean = readOnly { rw =>
    rw.has(ConfidentialInputCFKeys.confidentialInputByCommitment(commitment))
  }

  def getOutput(commitment: Commitment): Option[ConfidentialOutput] = readOnly { rw =>
    rw.get(ConfidentialOutputCFKeys.confidentialOutputByCommitment(commitment))
  }

  def outputExists(commitment: Commitment): Boolean = readOnly { rw =>
    rw.has(ConfidentialOutputCFKeys.confidentialOutputByCommitment(commitment))
  }

  val pendingItemsSet: ConfidentialRocksDBSet[ConfidentialDataId] = ConfidentialDataCFKeys.pendingConfidentialItemsSet(this)

  def pendingDataItems(): Set[ConfidentialDataId] = {
    pendingItemsSet.members
  }

  def addToPending(items: Set[ConfidentialDataId]): Int = {
    pendingItemsSet.add(items)
  }

  val lostItemsSet: ConfidentialRocksDBSet[ConfidentialDataId] = ConfidentialDataCFKeys.lostConfidentialItemsSet(this)
  def lostDataItems(): Set[ConfidentialDataId] = {
    lostItemsSet.members
  }

  def removeFromPendingAndLost(item: ConfidentialDataId): (Boolean, Boolean) =
    (pendingItemsSet.remove(item), lostItemsSet.remove(item))

  def pendingToLost(item: ConfidentialDataId): (Boolean, Boolean) =
    pendingItemsSet.remove(item) -> lostItemsSet.add(item)

  def contractDataItems(contractId: ContractId): Set[ConfidentialContractDataId] = ???

  def dataExists(commitment: Commitment, dataType: ConfidentialDataType): Boolean = {
    dataType match {
      case ConfidentialDataType.Input  => inputExists(commitment)
      case ConfidentialDataType.Output => outputExists(commitment)
    }
  }

  override protected def buildReadOnlyDb(db: RocksDB,
                                         readOptions: ReadOptions,
                                         columnHandles: Map[ConfidentialDBColumnFamily, ColumnFamilyHandle]): ConfidentialReadOnlyDB =
    new ConfidentialReadOnlyDB(db, readOptions, columnHandles)

  override protected def buildReadWriteDb(db: RocksDB,
                                          readOptions: ReadOptions,
                                          batch: WriteBatch,
                                          columnHandles: Map[ConfidentialDBColumnFamily, ColumnFamilyHandle]): ConfidentialReadWriteDB =
    new ConfidentialReadWriteDB(db, readOptions, batch, columnHandles)
}

object ConfidentialRocksDBStorage extends ScorexLogging {
  RocksDB.loadLibrary()

  def openDB(path: String, migrateScheme: Boolean = true, params: RocksDBParams = DefaultParams): ConfidentialRocksDBStorage = {
    log.debug(s"Open confidential DB at '$path'")
    val storage = openRocksDB(path, params)
    if (migrateScheme) {
      try {
        new ConfidentialSchemaManager(storage).migrateSchema()
      } catch {
        case NonFatal(e) =>
          closeQuietly(storage)
          throw e
      }
    }
    storage
  }

  private def openRocksDB(path: String, params: RocksDBParams): ConfidentialRocksDBStorage = {

    val (db, columnHandlesMap, parameters, stats, options) =
      prepareToCreateDB(path, params, ConfidentialDBColumnFamily, ConfidentialDBColumnFamily.PresetCF)

    new ConfidentialRocksDBStorage(db, columnHandlesMap, parameters, stats, options)
  }
}

class ConfidentialReadOnlyDB(val db: RocksDB, val readOptions: ReadOptions, val columnHandles: Map[ConfidentialDBColumnFamily, ColumnFamilyHandle])
    extends BaseReadOnlyDB[ConfidentialDBColumnFamily]

class ConfidentialReadWriteDB(db: RocksDB,
                              readOptions: ReadOptions,
                              val batch: WriteBatch,
                              columnHandles: Map[ConfidentialDBColumnFamily, ColumnFamilyHandle])
    extends ConfidentialReadOnlyDB(db, readOptions, columnHandles) with BaseReadWriteDB[ConfidentialDBColumnFamily]
