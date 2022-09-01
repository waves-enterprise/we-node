package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.{SponsoredFeesSupport, UnsignedTxRequest}
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.docker.CreateContractTransactionV2
import play.api.libs.json.{Format, JsNumber, JsObject, Json}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CreateContractTransactionV2]] request
  */
case class CreateContractRequestV2(version: Int,
                                   sender: String,
                                   image: String,
                                   imageHash: String,
                                   contractName: String,
                                   params: List[DataEntry[_]],
                                   fee: Long,
                                   feeAssetId: Option[String],
                                   timestamp: Option[Long] = None,
                                   password: Option[String] = None)
    extends UnsignedTxRequest
    with SponsoredFeesSupport {
  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(CreateContractTransactionV2.typeId.toInt))
}

object CreateContractRequestV2 {

  implicit val format: Format[CreateContractRequestV2] = Json.format

}
