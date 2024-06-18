package com.wavesenterprise.api.http.service.confidentialcontract

import com.wavesenterprise.state.contracts.confidential.{ConfidentialInput, ConfidentialOutput}
import com.wavesenterprise.transaction.docker.{ExecutedContractTransaction, ExecutedContractTransactionV4, ExecutedContractTransactionV5}
import play.api.libs.json.{Json, Writes}

sealed trait ConfidentialExecutableTxResponse {
  def executableTx: ExecutedContractTransaction
  def confidentialInput: ConfidentialInput
  def confidentialOutput: ConfidentialOutput
}

object ConfidentialExecutableTxResponse {
  implicit val writes: Writes[ConfidentialExecutableTxResponse] = Writes.apply {
    case v4: ConfidentialExecutableTxV4Response =>
      ConfidentialExecutableTxV4Response.confidentialExecutableTxV4Response.writes(v4)
    case v5: ConfidentialExecutableTxV5Response =>
      ConfidentialExecutableTxV5Response.confidentialExecutableV5Response.writes(v5)
  }
}

case class ConfidentialExecutableTxV4Response(
    executableTx: ExecutedContractTransactionV4,
    confidentialInput: ConfidentialInput,
    confidentialOutput: ConfidentialOutput
) extends ConfidentialExecutableTxResponse

object ConfidentialExecutableTxV4Response {
  implicit val executedContractTransactionV4: Writes[ExecutedContractTransactionV4] = tx => tx.json()
  implicit val confidentialExecutableTxV4Response: Writes[ConfidentialExecutableTxV4Response] =
    Json.writes[ConfidentialExecutableTxV4Response]
}

case class ConfidentialExecutableTxV5Response(
    executableTx: ExecutedContractTransactionV5,
    confidentialInput: ConfidentialInput,
    confidentialOutput: ConfidentialOutput
) extends ConfidentialExecutableTxResponse

object ConfidentialExecutableTxV5Response {
  implicit val executedContractTransactionV5: Writes[ExecutedContractTransactionV5] = tx => tx.json()
  implicit val confidentialExecutableV5Response: Writes[ConfidentialExecutableTxV5Response] =
    Json.writes[ConfidentialExecutableTxV5Response]

}
