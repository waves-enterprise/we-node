package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.docker.CreateContractTransactionV3
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CreateContractTransactionV3]] request
  */
case class CreateContractRequestV3(version: Int,
                                   sender: String,
                                   image: String,
                                   imageHash: String,
                                   contractName: String,
                                   params: List[DataEntry[_]],
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   timestamp: Option[Long] = None,
                                   atomicBadge: Option[AtomicBadge],
                                   password: Option[String] = None)
    extends UnsignedTxRequest
    with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CreateContractTransactionV3.typeId.toInt))
}

object CreateContractRequestV3 {

  implicit val format: Format[CreateContractRequestV3] = Json.format

}
