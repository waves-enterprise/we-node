package com.wavesenterprise.api.http.service.confidentialcontract

import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.transaction.{AtomicInnerTransaction, PaymentsV1ToContract}
import com.wavesenterprise.transaction.docker.CallContractTransaction
import play.api.libs.json._

case class ConfidentialContractCallResponse(
    callContractTransaction: CallContractTransaction with AtomicInnerTransaction with PaymentsV1ToContract,
    confidentialInput: ConfidentialInput,
    additionalMessage: String
)

object ConfidentialContractCallResponse {
  implicit val callContractTransaction: Writes[CallContractTransaction with AtomicInnerTransaction with PaymentsV1ToContract] = tx => tx.json()
  implicit val confidentialContractCallResponse: Writes[ConfidentialContractCallResponse]                                     = Json.writes[ConfidentialContractCallResponse]
}
