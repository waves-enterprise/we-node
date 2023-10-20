package com.wavesenterprise.state.contracts.confidential

import cats.implicits._
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialDiff
import com.wavesenterprise.state.{ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ExecutedContractData

class CompositeConfidentialState(underlying: ConfidentialState, maybeDiff: Option[ConfidentialDiff]) extends ConfidentialState {

  private def diff: ConfidentialDiff = maybeDiff.orEmpty

  override def contractKeys(request: KeysRequest): Vector[String] = {
    val contractId = request.contractId
    val keysFromDiff = diff.contractsData
      .get(ContractId(contractId))
      .map(_.data.keys.toVector)
      .orEmpty

    underlying.contractKeys(request.copy(knownKeys = keysFromDiff))
  }

  override def contractData(contractId: ContractId): ExecutedContractData = {
    val fromUnderlying = underlying.contractData(contractId)
    val fromDiff       = diff.contractsData.get(contractId).orEmpty

    fromUnderlying combine fromDiff
  }

  override def contractData(contractId: ContractId, keys: Iterable[String]): ExecutedContractData = {
    val fromUnderlying = underlying.contractData(contractId, keys)
    val fromDiff       = diff.contractsData.get(contractId).map(_.filterKeys(keys.toSet)).orEmpty

    fromUnderlying combine fromDiff
  }

  override def contractData(contractId: ContractId, key: String): Option[DataEntry[_]] = {
    diff.contractsData
      .get(contractId)
      .orEmpty
      .data.get(key)
      .orElse(underlying.contractData(contractId, key))
  }
}

object CompositeConfidentialState {
  def composite(underlying: ConfidentialState, maybeDiff: Option[ConfidentialDiff]) =
    new CompositeConfidentialState(underlying, maybeDiff)

  def composite(underlying: ConfidentialState, diff: ConfidentialDiff) =
    new CompositeConfidentialState(underlying, Some(diff))
}
