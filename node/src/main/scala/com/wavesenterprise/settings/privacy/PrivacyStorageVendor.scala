package com.wavesenterprise.settings.privacy

import enumeratum.EnumEntry.Lowercase
import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

sealed trait PrivacyStorageVendor extends EnumEntry with Lowercase

object PrivacyStorageVendor extends Enum[PrivacyStorageVendor] {
  val values: IndexedSeq[PrivacyStorageVendor] = findValues

  def fromStr(str: String): PrivacyStorageVendor = withNameInsensitiveOption(str).getOrElse(Unknown)

  case object Postgres extends PrivacyStorageVendor
  case object S3       extends PrivacyStorageVendor
  case object Unknown  extends PrivacyStorageVendor
}
