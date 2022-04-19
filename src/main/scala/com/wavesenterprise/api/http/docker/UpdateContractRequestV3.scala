package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.transaction.AtomicBadge
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.UpdateContractTransactionV3]] request
  */
case class UpdateContractRequestV3(sender: String,
                                   contractId: String,
                                   image: String,
                                   imageHash: String,
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   timestamp: Option[Long] = None,
                                   atomicBadge: Option[AtomicBadge],
                                   password: Option[String] = None)
    extends UnsignedTxRequest
    with SponsoredFeesSupport

object UpdateContractRequestV3 {

  implicit val format: OFormat[UpdateContractRequestV3] = Json.format

}
