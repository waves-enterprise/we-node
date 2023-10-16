package com.wavesenterprise.network.contracts

import enumeratum.values.{ByteEnum, ByteEnumEntry}

import scala.collection.immutable

sealed abstract class ConfidentialDataType(val value: Byte) extends ByteEnumEntry

object ConfidentialDataType extends ByteEnum[ConfidentialDataType] {

  case object Input  extends ConfidentialDataType(0)
  case object Output extends ConfidentialDataType(1)

  override def values: immutable.IndexedSeq[ConfidentialDataType] = findValues

}
