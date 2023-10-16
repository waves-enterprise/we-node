package com.wavesenterprise.state.contracts.confidential

import com.wavesenterprise.crypto.internals.SaltBytes
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.{ByteStr, ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps
import play.api.libs.json.{Format, Json}

case class ConfidentialOutput(commitment: Commitment, txId: ByteStr, contractId: ContractId, commitmentKey: SaltBytes, entries: List[DataEntry[_]])
    extends ConfidentialDataUnit

object ConfidentialOutput {

  def fromBytes(bytes: Array[Byte]): ConfidentialOutput = {
    val (commitment, commitmentEnd)                   = BinarySerializer.parseShortByteStr(bytes)
    val (txId, txIdEnd)                               = BinarySerializer.parseShortByteStr(bytes, commitmentEnd)
    val (contractId, contractIdEnd)                   = BinarySerializer.parseShortByteStr(bytes, txIdEnd)
    val (commitmentKeyByteStr, commitmentKeyBytesEnd) = BinarySerializer.parseShortByteStr(bytes, contractIdEnd)
    val (params, _)                                   = BinarySerializer.parseShortList(bytes, ContractTransactionEntryOps.parse, commitmentKeyBytesEnd)
    ConfidentialOutput(Commitment(commitment), txId, ContractId(contractId), SaltBytes(commitmentKeyByteStr), params)
  }

  import ConfidentialInput._

  implicit val confidentialOutputFormat: Format[ConfidentialOutput] = Json.format
}
