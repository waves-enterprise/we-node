package com.wavesenterprise.metrics

import enumeratum.values.{StringEnum, StringEnumEntry}
import play.api.libs.json._

import scala.collection.immutable

sealed abstract class MetricsType(val value: String) extends StringEnumEntry

object MetricsType extends StringEnum[MetricsType] {

  implicit val MetricsTypeReads: Reads[MetricsType]   = (json: JsValue) => JsSuccess(withValue(json.as[JsString].value))
  implicit val MetricsTypeWrites: Writes[MetricsType] = (o: MetricsType) => JsString(o.value)

  case object Common             extends MetricsType("common")
  case object Block              extends MetricsType("block")
  case object Privacy            extends MetricsType("privacy")
  case object Contract           extends MetricsType("contract")
  case object ContractValidation extends MetricsType("contract-validation")
  case object PoA                extends MetricsType("poa")

  override def values: immutable.IndexedSeq[MetricsType] = findValues

  override def withValue(i: String): MetricsType            = super.withValue(i.toLowerCase())
  override def withValueOpt(i: String): Option[MetricsType] = super.withValueOpt(i.toLowerCase())
}
