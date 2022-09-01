package com.wavesenterprise.api.http.service

import com.wavesenterprise.anchoring.AnchoringConfiguration
import play.api.libs.json.{Json, OFormat}

class AnchoringApiService(anchoringConfiguration: AnchoringConfiguration) {

  def config: AnchoringConfigResponse = {
    anchoringConfiguration match {
      case enabled: AnchoringConfiguration.EnabledAnchoringCfg =>
        AnchoringConfigResponse(
          currentChainOwnerAddress = enabled.currentChainPrivateKey.address,
          targetnetRecipientAddress = enabled.targetnetRecipientAddress,
          targetnetNodeAddress = enabled.targetnetNodeAddress.toString(),
          targetnetSchemeByte = enabled.targetnetSchemeByte.toChar.toString,
          targetnetFee = enabled.targetnetFee,
          currentChainFee = enabled.currentChainFee,
          heightRange = enabled.heightCondition.range,
          heightAbove = enabled.heightCondition.above,
          threshold = enabled.threshold,
        )

      case AnchoringConfiguration.DisabledAnchoringCfg =>
        AnchoringConfigResponse.disabled
    }
  }
}

case class AnchoringConfigResponse(
    enabled: Boolean,
    currentChainOwnerAddress: Option[String],
    targetnetNodeAddress: Option[String],
    targetnetSchemeByte: Option[String],
    targetnetRecipientAddress: Option[String],
    targetnetFee: Option[Long],
    currentChainFee: Option[Long],
    heightRange: Option[Int],
    heightAbove: Option[Int],
    threshold: Option[Int]
)

object AnchoringConfigResponse {
  def apply(
      currentChainOwnerAddress: String,
      targetnetNodeAddress: String,
      targetnetSchemeByte: String,
      targetnetRecipientAddress: String,
      targetnetFee: Long,
      currentChainFee: Long,
      heightRange: Int,
      heightAbove: Int,
      threshold: Int
  ): AnchoringConfigResponse = {
    new AnchoringConfigResponse(
      enabled = true,
      Some(currentChainOwnerAddress),
      Some(targetnetNodeAddress),
      Some(targetnetSchemeByte),
      Some(targetnetRecipientAddress),
      Some(targetnetFee),
      Some(currentChainFee),
      Some(heightRange),
      Some(heightAbove),
      Some(threshold)
    )
  }

  def disabled: AnchoringConfigResponse = {
    new AnchoringConfigResponse(enabled = false, None, None, None, None, None, None, None, None, None)
  }

  implicit val anchoringConfigResponseFormat: OFormat[AnchoringConfigResponse] = Json.format
}
