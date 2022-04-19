package com.wavesenterprise.api.http

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.Hyphencase
import play.api.libs.json.{JsObject, Json, OWrites}

import scala.collection.immutable

sealed trait StatusResponse {
  def isFrozen: Option[Boolean]
}

object StatusResponse {
  implicit val writes: OWrites[StatusResponse] = {
    case s: NodeStatusResponse     => NodeStatusResponse.format.writes(s)
    case FrozenStatusResponse      => FrozenStatusResponse.format.writes(FrozenStatusResponse)
    case s: ExternalStatusResponse => ExternalStatusResponse.format.writes(s)
  }
}

case class NodeStatusResponse(blockchainHeight: Int, stateHeight: Int, updatedTimestamp: Long, updatedDate: String, lastCheckTimestamp: Long)
    extends StatusResponse {
  override val isFrozen: Option[Boolean] = None
}

object NodeStatusResponse {
  val format: OWrites[NodeStatusResponse] = Json.writes[NodeStatusResponse]
}

case object FrozenStatusResponse extends StatusResponse {
  override val isFrozen: Option[Boolean] = Some(true)

  val format: OWrites[FrozenStatusResponse.type] = Json.writes[FrozenStatusResponse.type]
}

sealed abstract class ExternalStatusResponse extends StatusResponse with EnumEntry with Hyphencase {
  override val isFrozen: Option[Boolean] = None
}

object ExternalStatusResponse extends Enum[ExternalStatusResponse] {
  val values: immutable.IndexedSeq[ExternalStatusResponse] = findValues

  val format: OWrites[ExternalStatusResponse] = (_: ExternalStatusResponse) => JsObject.empty

  def fromStr(str: String): ExternalStatusResponse = withNameInsensitiveOption(str).getOrElse(Unknown)

  case object Docker         extends ExternalStatusResponse
  case object PrivacyStorage extends ExternalStatusResponse
  case object AnchoringAuth  extends ExternalStatusResponse
  case object Unknown        extends ExternalStatusResponse
}
