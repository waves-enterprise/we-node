package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.transaction.AtomicBadge
import play.api.libs.json.{Json, OFormat}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.DisableContractTransactionV3]] request
  */
case class DisableContractRequestV3(sender: String,
                                    contractId: String,
                                    fee: Long,
                                    feeAssetId: Option[String],
                                    timestamp: Option[Long] = None,
                                    atomicBadge: Option[AtomicBadge],
                                    password: Option[String] = None)
    extends UnsignedTxRequest
    with SponsoredFeesSupport

object DisableContractRequestV3 {

  implicit val format: OFormat[DisableContractRequestV3] = Json.format

}
