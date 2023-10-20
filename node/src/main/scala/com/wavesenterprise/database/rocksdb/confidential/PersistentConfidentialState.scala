package com.wavesenterprise.database.rocksdb.confidential

import com.wavesenterprise.block.Block
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.database.docker.{KeysPagination, KeysRequest}
import com.wavesenterprise.database.rocksdb.RocksDBWriter.BaseReadOnlyDBExt
import com.wavesenterprise.database.rocksdb.{CollectKeysToDiscardOps, ConfidentialDBColumnFamily, HistoryUpdater}
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.state.contracts.confidential.ConfidentialState
import com.wavesenterprise.state.{ContractId, DataEntry}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.utils.ScorexLogging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PersistentConfidentialState(val storage: ConfidentialRocksDBStorage, val maxRollbackDepth: Int)
    extends ConfidentialState
    with ConfidentialReadWrite
    with HistoryUpdater[ConfidentialDBColumnFamily, ConfidentialReadOnlyDB, ConfidentialReadWriteDB]
    with CollectKeysToDiscardOps
    with ScorexLogging {

  import PersistentConfidentialState._

  def lastPersistenceBlock: Option[BlockId] = readOnly { ro =>
    ro.get(ConfidentialStateCFKeys.lastPersistedBlock)
  }

  override def contractKeys(request: KeysRequest): Vector[String] = readOnly { db =>
    val KeysRequest(contractIdByteStr, offsetOpt, limitOpt, keysFilter, knownKeys) = request

    val keys       = ConfidentialDBKeys.contractKeys(ContractId(contractIdByteStr), storage).members(db)
    val pagination = new KeysPagination((keys ++ knownKeys).iterator)
    pagination.paginatedKeys(offsetOpt, limitOpt, keysFilter).toVector
  }

  override def contractData(contractId: ContractId): ExecutedContractData = readOnly { db =>
    ExecutedContractData((for {
      key   <- ConfidentialDBKeys.contractKeys(contractId, storage).members(db)
      value <- contractKeyData(db, contractId, key)
    } yield key -> value).toMap)
  }

  override def contractData(contractId: ContractId, keys: Iterable[String]): ExecutedContractData = readOnly {
    db =>
      ExecutedContractData((for {
        key   <- keys
        value <- contractKeyData(db, contractId, key)
      } yield key -> value).toMap)
  }

  override def contractData(contractId: ContractId, key: String): Option[DataEntry[_]] =
    readOnly(contractKeyData(_: ConfidentialReadOnlyDB, contractId, key))

  private def contractKeyData(db: ConfidentialReadOnlyDB, contractId: ContractId, key: String): Option[DataEntry[_]] =
    db.fromHistory(ConfidentialDBKeys.contractDataHistory(contractId, key), ConfidentialDBKeys.contractData(contractId, key)).flatten

  def appendBlock(blockDiff: ConfidentialDiff, id: BlockId, height: Int): Unit = readWrite { rw =>
    val expiredKeys = new ArrayBuffer[(ConfidentialDBColumnFamily, Seq[Array[Byte]])]

    val contractsData: Map[ContractId, ExecutedContractData] = blockDiff match {
      case ConfidentialDiffValue(contractsData) => contractsData
      case EmptyConfidentialDiff                => Map.empty
    }

    val threshold = height - maxRollbackDepth

    for ((contractId, contractData) <- contractsData) {
      val contractKeys = ConfidentialDBKeys.contractKeys(contractId, storage)
      val newKeys = for {
        (key, value) <- contractData.data
        historyKey = ConfidentialDBKeys.contractDataHistory(contractId, key)
        _          = rw.put(ConfidentialDBKeys.contractData(contractId, key)(height), Some(value))
        _          = expiredKeys += updateHistory(rw, historyKey, threshold, ConfidentialDBKeys.contractData(contractId, key))(height)
        isNew      = !contractKeys.contains(rw, key)
        if isNew
      } yield key
      if (newKeys.nonEmpty) {
        contractKeys.add(rw, newKeys)
      }
    }

    expiredKeys.foreach {
      case (columnFamily, keys) => rw.delete(columnFamily, keys)
    }

    rw.put(ConfidentialStateCFKeys.lastPersistedBlock, Some(id))
  }

  def rollbackBlock(block: Block, height: Int): Unit = {
    val contractsKeysToDiscard = mutable.Map[ContractId, Set[String]]()
    for (tx <- block.transactionData) {
      tx match {
        case tx: ExecutedContractTransactionV4 =>
          tx.tx match {
            case _: CreateContractTransaction =>
              collectKeysToDiscard(tx, contractsKeysToDiscard)

            case _: CallContractTransaction =>
              collectKeysToDiscard(tx, contractsKeysToDiscard)

            case _: UpdateContractTransaction =>
          }
        case _ =>
      }
    }
    readWrite { rw =>
      contractsKeysToDiscard.foreach {
        case (contractId, keys) => rollbackContractData(rw, contractId, keys, height)
      }
      rw.put(ConfidentialStateCFKeys.lastPersistedBlock, Some(block.reference))
    }
  }

  private def rollbackContractData(rw: ConfidentialReadWriteDB, contractId: ContractId, keys: Set[String], currentHeight: Int): Unit = {
    val contractKeys = ConfidentialDBKeys.contractKeys(contractId, storage)
    val orphanedKeys = for {
      key <- keys
      historyKey = ConfidentialDBKeys.contractDataHistory(contractId, key)
      filtered = {
        log.trace(s"Discarding '$key' for '$contractId' contract at '$currentHeight'")
        rw.delete(ConfidentialDBKeys.contractData(contractId, key)(currentHeight))
        rw.filterHistory(historyKey, currentHeight)
      }
      if filtered.isEmpty
    } yield {
      rw.delete(historyKey)
      key
    }
    if (orphanedKeys.nonEmpty) {
      contractKeys.remove(rw, orphanedKeys)
    }
  }
}

object PersistentConfidentialState {

  def apply(storage: ConfidentialRocksDBStorage, settings: WESettings): PersistentConfidentialState =
    new PersistentConfidentialState(
      storage,
      settings.additionalCache.confidentialRocksdb.maxRollbackDepth,
    )

  implicit class ConfidentialReadOnlyDBExt(override val db: ConfidentialReadOnlyDB)
      extends BaseReadOnlyDBExt[ConfidentialDBColumnFamily, ConfidentialReadOnlyDB](db)

}
