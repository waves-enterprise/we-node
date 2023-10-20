package com.wavesenterprise.api.http.service.confidentialcontract

import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.transaction.docker.CallContractTransactionV6
import play.api.libs.json._

case class ConfidentialContractCallResponse(
    callContractTransactionV6: CallContractTransactionV6,
    confidentialInput: ConfidentialInput
)

object ConfidentialContractCallResponse {
  implicit val callContractTransactionV6: Writes[CallContractTransactionV6]               = tx => tx.json()
  implicit val confidentialContractCallResponse: Writes[ConfidentialContractCallResponse] = Json.writes[ConfidentialContractCallResponse]
}
