package com.wavesenterprise.database.rocksdb.confidential

import com.google.common.primitives.Shorts
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.database.KeyHelpers.{bytes, hBytes}
import com.wavesenterprise.database.{RocksDBDeque, RocksDBSet}
import com.wavesenterprise.database.RocksDBSet._
import com.wavesenterprise.database.rocksdb.ConfidentialDBColumnFamily
import com.wavesenterprise.database.rocksdb.ConfidentialDBColumnFamily.ConfidentialDataSyncCF
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialKeyHelpers.historyKey
import com.wavesenterprise.network.contracts.{ConfidentialDataId, ConfidentialDataType}
import com.wavesenterprise.state.contracts.confidential.ConfidentialStateUpdater.PersistenceBlockTask
import com.wavesenterprise.state.contracts.confidential.{ConfidentialInput, ConfidentialOutput}
import com.wavesenterprise.state.{ByteStr, ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

object ConfidentialDataCFKeys {
  private val PendingItemsPrefix: Short = 1
  private val LostItemsPrefix: Short    = 2

  def pendingConfidentialItemsSet(storage: ConfidentialRocksDBStorage): ConfidentialRocksDBSet[ConfidentialDataId] = {
    confidentialItemsSet(storage, "pending-confidential-items", PendingItemsPrefix)
  }

  def lostConfidentialItemsSet(storage: ConfidentialRocksDBStorage): ConfidentialRocksDBSet[ConfidentialDataId] = {
    confidentialItemsSet(storage, "lost-confidential-items", LostItemsPrefix)
  }

  private def confidentialItemsSet(storage: ConfidentialRocksDBStorage, name: String, prefix: Short): ConfidentialRocksDBSet[ConfidentialDataId] = {
    RocksDBSet.newConfidential(
      name = name,
      columnFamily = ConfidentialDataSyncCF,
      prefix = bytes(prefix, Array.emptyByteArray),
      storage = storage,
      itemEncoder = writeConfidentialDataId,
      itemDecoder = readConfidentialDataId
    )
  }

  private def writeConfidentialDataId(confidentialDataId: ConfidentialDataId): Array[Byte] = {
    Array.concat(
      confidentialDataId.contractId.byteStr.arr,
      confidentialDataId.commitment.hash.arr,
      Array(confidentialDataId.dataType.value)
    )
  }

  private def readConfidentialDataId(bytes: Array[Byte]): ConfidentialDataId = {
    val inputBuffer = ByteBuffer.wrap(bytes)

    val contractBytes = new Array[Byte](com.wavesenterprise.crypto.DigestSize)
    inputBuffer.get(contractBytes)

    val commitmentBytes = new Array[Byte](com.wavesenterprise.crypto.DigestSize)
    inputBuffer.get(commitmentBytes)

    val dataType = ConfidentialDataType.withValue(inputBuffer.get())

    ConfidentialDataId(ContractId(ByteStr(contractBytes)), Commitment(ByteStr(commitmentBytes)), dataType)
  }

}

object ConfidentialInputCFKeys {
  private[this] val InputByCommitmentPrefix: Short = 1

  val inputParser: Array[Byte] => ConfidentialInput  = ConfidentialInput.fromBytes
  val inputEncoder: ConfidentialInput => Array[Byte] = confidentialInput => confidentialInput.toBytes

  def confidentialInputByCommitment(commitment: Commitment): ConfidentialKey[Option[ConfidentialInput]] =
    ConfidentialDBKey.opt(
      "confidential-input-by-commitment",
      ConfidentialDBColumnFamily.ConfidentialInputCF,
      bytes(InputByCommitmentPrefix, commitment.hash.arr),
      inputParser,
      inputEncoder
    )
}

object ConfidentialOutputCFKeys {
  private[this] val OutputByCommitmentPrefix: Short = 1

  val outputParser: Array[Byte] => ConfidentialOutput  = ConfidentialOutput.fromBytes
  val outputEncoder: ConfidentialOutput => Array[Byte] = confidentialOutput => confidentialOutput.toBytes

  def confidentialOutputByCommitment(commitment: Commitment): ConfidentialKey[Option[ConfidentialOutput]] =
    ConfidentialDBKey.opt(
      "confidential-output-by-commitment",
      ConfidentialDBColumnFamily.ConfidentialOutputCF,
      bytes(OutputByCommitmentPrefix, commitment.hash.arr),
      outputParser,
      outputEncoder
    )
}

object ConfidentialStateCFKeys {

  private[this] val ContractDataHistoryPrefix: Short = 1
  private[this] val ContractDataPrefix: Short        = 2
  private[this] val UpdaterBlockQueue: Short         = 3
  private[this] val LastPersistedBlock: Short        = 4

  def contractDataHistory(contractId: ContractId, key: String): ConfidentialKey[Seq[Int]] =
    historyKey(
      "contract-data-history",
      ConfidentialDBColumnFamily.ConfidentialStateCF,
      ContractDataHistoryPrefix,
      contractId.byteStr.arr ++ key.getBytes(UTF_8)
    )

  def contractData(contractId: ContractId, key: String, height: Int): ConfidentialKey[Option[DataEntry[_]]] =
    ConfidentialDBKey.opt(
      "contract-data",
      ConfidentialDBColumnFamily.ConfidentialStateCF,
      hBytes(ContractDataPrefix, height, contractId.byteStr.arr ++ key.getBytes(UTF_8)),
      ContractTransactionEntryOps.parseValue(key, _, 0)._1,
      ContractTransactionEntryOps.valueBytes
    )

  def persistenceBlockQueue(state: PersistentConfidentialState) = RocksDBDeque.newConfidential[PersistenceBlockTask](
    name = "confidential_block_queue",
    prefix = Shorts.toByteArray(UpdaterBlockQueue),
    storage = state.storage,
    itemEncoder = (item: PersistenceBlockTask) => item.bytes(),
    itemDecoder = PersistenceBlockTask.parseBytesUnsafe(_),
    columnFamily = ConfidentialDBColumnFamily.ConfidentialStateCF
  )

  val lastPersistedBlock =
    ConfidentialDBKey.opt[BlockId]("last_persisted_key", Shorts.toByteArray(LastPersistedBlock), ByteStr.apply(_), (id: ByteStr) => id.arr)
}
