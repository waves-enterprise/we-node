package com.wavesenterprise.network.contracts

import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.state.ContractId

case class ConfidentialDataId(contractId: ContractId, commitment: Commitment, dataType: ConfidentialDataType) {
  override def toString: String = s"ConfidentialDataId(contractId: '$contractId', commitment: '${commitment.hash}', dataType: '$dataType')"
}
