package com.wavesenterprise.state.contracts.confidential

import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.state.{ContractId, DataEntry}
import com.wavesenterprise.transaction.docker.ExecutedContractData

trait ConfidentialState {
  def contractKeys(request: KeysRequest): Vector[String]

  def contractData(contractId: ContractId): ExecutedContractData

  def contractData(contractId: ContractId, keys: Iterable[String]): ExecutedContractData

  def contractData(contractId: ContractId, key: String): Option[DataEntry[_]]
}
