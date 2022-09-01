package com.wavesenterprise.state

import com.google.protobuf.{ByteString => PbByteString}
import com.wavesenterprise.block.Block
import com.wavesenterprise.protobuf.service.messagebroker.{
  AppendedBlockHistory => PbAppendedBlockHistory,
  BlockAppended => PbBlockAppended,
  BlockchainEvent => PbBlockchainEvent,
  MicroBlockAppended => PbMicroBlockAppended,
  RollbackCompleted => PbRollbackCompleted
}
import com.wavesenterprise.state.BlockchainEvent._
import com.wavesenterprise.transaction.{ProtoSerializableTransaction, Transaction}

sealed trait BlockchainEvent {
  def toProto: PbBlockchainEvent
}

object BlockchainEvent {
  implicit def byteStrToPbByteString(byteStr: ByteStr): PbByteString      = PbByteString.copyFrom(byteStr.arr)
  implicit def pbByteStringToByteStr(pbByteString: PbByteString): ByteStr = ByteStr(pbByteString.toByteArray)
}

sealed trait EventResult extends BlockchainEvent {
  def transactions: Seq[Transaction]
  def withTransactions(txs: Seq[Transaction]): EventResult
}

final case class BlockAppended(blockSignature: ByteStr,
                               reference: ByteStr,
                               transactions: Seq[Transaction],
                               minerAddress: ByteStr,
                               height: Int,
                               version: Byte,
                               timestamp: Long,
                               fee: Long,
                               blockSize: Int,
                               features: Set[Short])
    extends EventResult {

  override def toString: String =
    s"BlockAppended($blockSignature -> $reference, version=$version, miner=$minerAddress, txs=${transactions.size}, height=$height, ts=$timestamp, size=$blockSize)"

  override def toProto: PbBlockchainEvent = {
    val blockAppended = PbBlockAppended(
      blockSignature,
      reference,
      transactions.map(tx => implicitly[PbByteString](tx.id())),
      minerAddress,
      height,
      version,
      timestamp,
      fee,
      blockSize,
      features.map(_.toInt).toSeq
    )
    PbBlockchainEvent(PbBlockchainEvent.BlockchainEvent.BlockAppended(blockAppended))
  }

  override def withTransactions(txs: Seq[Transaction]): EventResult = this.copy(transactions = txs)

  def toHistoryEvent: AppendedBlockHistory = AppendedBlockHistory(
    blockSignature,
    reference,
    transactions,
    minerAddress,
    height,
    version,
    timestamp,
    fee,
    blockSize,
    features
  )
}

object BlockAppended {
  def apply(block: Block, blockHeight: Int): BlockAppended =
    BlockAppended(
      block.uniqueId,
      block.reference,
      block.transactionData,
      block.signerData.generator.toAddress.bytes,
      blockHeight,
      block.version,
      block.timestamp,
      block.blockFee(),
      block.bytes().length,
      block.featureVotes
    )
}

final case class MicroBlockAppended(transactions: Seq[Transaction]) extends EventResult {

  override def toString: String = s"MicroBlockAppended(txs=${transactions.size})"

  override def toProto: PbBlockchainEvent = {
    val microBlockAppended = PbMicroBlockAppended(transactions.collect {
      case transaction: ProtoSerializableTransaction => transaction.toProto
    })
    PbBlockchainEvent(PbBlockchainEvent.BlockchainEvent.MicroBlockAppended(microBlockAppended))
  }

  override def withTransactions(txs: Seq[Transaction]): EventResult = this.copy(transactions = txs)
}

final case class RollbackCompleted(returnToSignature: ByteStr, transactions: Seq[Transaction]) extends EventResult {

  override def toString: String = s"RollbackCompleted(to=$returnToSignature, txs=${transactions.size}"

  override def toProto: PbBlockchainEvent = {
    val rollbackCompleted = PbRollbackCompleted(returnToSignature, transactions.map(tx => implicitly[PbByteString](tx.id())))
    PbBlockchainEvent(PbBlockchainEvent.BlockchainEvent.RollbackCompleted(rollbackCompleted))
  }

  override def withTransactions(txs: Seq[Transaction]): EventResult = this.copy(transactions = txs)
}

final case class AppendedBlockHistory(
    blockSignature: ByteStr,
    reference: ByteStr,
    transactions: Seq[Transaction],
    minerAddress: ByteStr,
    height: Int,
    version: Byte,
    timestamp: Long,
    fee: Long,
    blockSize: Int,
    features: Set[Short]
) extends EventResult {

  override def toString: String =
    s"AppendedBlockHistory($blockSignature -> $reference, version=$version, height=$height, miner=$minerAddress, txs=${transactions.size}, ts=$timestamp, size=$blockSize)"

  override def toProto: PbBlockchainEvent = {
    val appendedBlockHistory = PbAppendedBlockHistory(
      blockSignature,
      reference,
      transactions.collect { case transaction: ProtoSerializableTransaction => transaction.toProto },
      minerAddress,
      height,
      version,
      timestamp,
      fee,
      blockSize,
      features.map(_.toInt).toSeq
    )
    PbBlockchainEvent(PbBlockchainEvent.BlockchainEvent.AppendedBlockHistory(appendedBlockHistory))
  }

  override def withTransactions(txs: Seq[Transaction]): EventResult = this.copy(transactions = txs)
}

object AppendedBlockHistory {
  def apply(block: Block, blockHeight: Int): AppendedBlockHistory =
    AppendedBlockHistory(
      block.uniqueId,
      block.reference,
      block.transactionData,
      block.signerData.generator.toAddress.bytes,
      blockHeight,
      block.version,
      block.timestamp,
      block.blockFee(),
      block.bytes().length,
      block.featureVotes
    )
}
