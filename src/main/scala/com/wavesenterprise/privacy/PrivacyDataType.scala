package com.wavesenterprise.privacy

import enumeratum.values.{ByteEnum, ByteEnumEntry}

import scala.collection.immutable

sealed abstract class PrivacyDataType(val value: Byte) extends ByteEnumEntry

object PrivacyDataType extends ByteEnum[PrivacyDataType] {
  case object Default extends PrivacyDataType(0)
  case object Large   extends PrivacyDataType(1)

  override def values: immutable.IndexedSeq[PrivacyDataType] = findValues
}
