package com.wavesenterprise.api.http.service.confidentialcontract

import com.wavesenterprise.state.contracts.confidential.{ConfidentialInput, ConfidentialOutput}
import com.wavesenterprise.transaction.docker.ExecutedContractTransactionV4
import play.api.libs.json.{Json, Writes}

case class ConfidentialTxByExecutableTxIdResponse(
    executedContractTransactionV4: ExecutedContractTransactionV4,
    confidentialInput: ConfidentialInput,
    confidentialOutput: ConfidentialOutput
)

object ConfidentialTxByExecutableTxIdResponse {
  implicit val executedContractTransactionV4: Writes[ExecutedContractTransactionV4] = tx => tx.json()
  implicit val confidentialTxByExecutableTxIdResponse: Writes[ConfidentialTxByExecutableTxIdResponse] =
    Json.writes[ConfidentialTxByExecutableTxIdResponse]
}
