package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.DisableContractTransactionV2]] request
  */
case class DisableContractRequestV2(sender: String,
                                    contractId: String,
                                    fee: Long,
                                    feeAssetId: Option[String],
                                    timestamp: Option[Long] = None,
                                    password: Option[String] = None)
    extends UnsignedTxRequest
    with SponsoredFeesSupport

object DisableContractRequestV2 {

  implicit val format: OFormat[DisableContractRequestV2] = Json.format

}
