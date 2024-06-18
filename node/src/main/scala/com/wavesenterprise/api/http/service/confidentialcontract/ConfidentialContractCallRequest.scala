package com.wavesenterprise.api.http.service.confidentialcontract

import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
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
    payments: List[ContractTransferInV1],
    commitment: Option[String],
    commitmentKey: Option[String],
    participantsForAtomic: List[String] = List.empty,
    certificatesBytes: List[Array[Byte]] = List.empty,
    callFunc: Option[String] = None,
    contractEngine: String = "docker"
)

object ConfidentialContractCallRequest {
  implicit val confidentialContractCallRequest: Format[ConfidentialContractCallRequest] =
    Json.format
}
