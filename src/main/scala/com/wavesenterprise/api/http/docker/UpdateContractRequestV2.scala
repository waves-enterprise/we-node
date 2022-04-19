package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.UpdateContractTransactionV2]] request
  */
case class UpdateContractRequestV2(sender: String,
                                   contractId: String,
                                   image: String,
                                   imageHash: String,
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   timestamp: Option[Long] = None,
                                   password: Option[String] = None)
    extends UnsignedTxRequest
    with SponsoredFeesSupport

object UpdateContractRequestV2 {

  implicit val format: OFormat[UpdateContractRequestV2] = Json.format

}
