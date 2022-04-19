package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.state.DataEntry
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CallContractTransactionV3]] request
  */
case class CallContractRequestV3(version: Int,
                                 sender: String,
                                 contractId: String,
                                 contractVersion: Int,
                                 params: List[DataEntry[_]],
                                 fee: Long,
                                 feeAssetId: Option[String],
                                 timestamp: Option[Long] = None,
                                 password: Option[String] = None)
    extends UnsignedTxRequest
    with SponsoredFeesSupport

object CallContractRequestV3 {

  implicit val format: OFormat[CallContractRequestV3] = Json.format

}
