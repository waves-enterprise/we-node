package com.wavesenterprise.api.http.privacy

import cats.Show
import com.wavesenterprise.api.decodeBase64Certificates
import com.wavesenterprise.api.http.BroadcastRequest
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.Validation
import com.wavesenterprise.transaction.{AssetIdStringLength, AtomicBadge, PolicyDataHashTransaction, ValidationError}
import play.api.libs.json.{JsNumber, JsObject, Json, OFormat}

sealed trait SendDataDTO {
  def version: Byte
  def sender: String
  def policyId: String
  def hash: String
  def info: PrivacyDataInfo
  def fee: Long
  def feeAssetId: Option[String]
  def atomicBadge: Option[AtomicBadge]
  def password: Option[String]
}

case class SendDataRequest(
    version: Byte,
    sender: String,
    policyId: String,
    data: Option[String],
    hash: String,
    info: PrivacyDataInfo,
    fee: Long,
    feeAssetId: Option[String] = None,
    atomicBadge: Option[AtomicBadge] = None,
    password: Option[String] = None,
    certificates: List[String] = List.empty
) extends SendDataDTO {

  def toPolicyItem: Either[ValidationError, PolicyItem] = {
    decodeBase64Certificates(certificates).map { decodedCerts =>
      PolicyItem(version, sender, policyId, Left(data.getOrElse("")), hash, info, fee, feeAssetId, atomicBadge, password, decodedCerts)
    }
  }

  def toJson: JsObject = Json.toJson(this).as[JsObject] + ("type" -> JsNumber(PolicyDataHashTransaction.typeId.toInt))
}

case class PolicyItem(
    version: Byte,
    sender: String,
    policyId: String,
    data: Either[String, ByteStr],
    hash: String,
    info: PrivacyDataInfo,
    fee: Long,
    feeAssetId: Option[String] = None,
    atomicBadge: Option[AtomicBadge] = None,
    password: Option[String] = None,
    certificatesBytes: List[Array[Byte]] = List.empty
) extends SendDataDTO
    with BroadcastRequest {

  def mergeWithRequest(req: SendDataRequest): PolicyItem =
    this.copy(
      version = req.version,
      sender = req.sender,
      policyId = req.policyId,
      hash = req.hash,
      info = req.info,
      fee = req.fee,
      feeAssetId = req.feeAssetId,
      atomicBadge = req.atomicBadge,
      password = req.password
    )

  def dataLength: Int = data match {
    case Left(d)  => d.length
    case Right(d) => d.arr.length
  }

  def certificatesCount: Int = certificatesBytes.size

  def parsedFeeAssetId: Validation[Option[ByteStr]] = parseBase58ToOption(feeAssetId, "invalid feeAssetId", AssetIdStringLength)

}

object SendDataRequest {
  implicit val jsonFormat: OFormat[SendDataRequest] = Json.using[Json.WithDefaultValues].format[SendDataRequest]
}

object PolicyItem {
  def apply(data: Either[String, ByteStr]): PolicyItem =
    new PolicyItem(0: Byte, "", "", data, "", PrivacyDataInfo("", 0, 0, "", ""), 0)

  implicit val toPrintable: Show[PolicyItem] = { x =>
    import x._

    s"""
       |sender: $sender
       |policyId: $policyId
       |data.length: $dataLength
       |hash: $hash
       |info: $info
       |fee: $fee
       |feeAssetId: $feeAssetId
       |atomicBadge: $atomicBadge
       |certificates.count: $certificatesCount""".stripMargin
  }
}
