package com.wavesenterprise.api.http.service.confidentialcontract

import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import play.api.libs.json.{Format, Json}

case class ConfidentialContractCallRequest(
    sender: String,
    password: Option[String] = None,
    contractId: String,
    contractVersion: Int,
    params: List[DataEntry[_]],
    timestamp: Long,
    atomicBadge: Option[AtomicBadge] = None,
    fee: Long,
    feeAssetId: Option[String] = None,
    commitment: Option[String],
    commitmentKey: Option[String],
    certificatesBytes: List[Array[Byte]] = List.empty
)

object ConfidentialContractCallRequest {
  implicit val confidentialContractCallRequest: Format[ConfidentialContractCallRequest] =
    Json.format
}
