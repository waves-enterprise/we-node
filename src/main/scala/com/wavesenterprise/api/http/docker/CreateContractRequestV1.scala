package com.wavesenterprise.api.http.docker

import com.wavesenterprise.api.http.UnsignedTxRequest
import com.wavesenterprise.state.DataEntry
import play.api.libs.json.{Format, Json}

/**
  * Unsigned [[com.wavesenterprise.transaction.docker.CreateContractTransactionV1]] request
  */
case class CreateContractRequestV1(sender: String,
                                   image: String,
                                   imageHash: String,
                                   contractName: String,
                                   params: List[DataEntry[_]],
                                   fee: Long,
                                   timestamp: Option[Long] = None,
                                   password: Option[String] = None)
    extends UnsignedTxRequest {}

object CreateContractRequestV1 {

  implicit val format: Format[CreateContractRequestV1] = Json.format

}
