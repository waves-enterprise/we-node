package com.wavesenterprise

import enumeratum.EnumEntry.Lowercase
import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

sealed trait CryptoType extends EnumEntry with Lowercase
object CryptoType extends Enum[CryptoType] {

  val values: immutable.IndexedSeq[CryptoType] = findValues
  def fromStr(str: String): CryptoType         = withNameInsensitiveOption(str).getOrElse(Unknown)

  case object Waves   extends CryptoType
  case object Unknown extends CryptoType
}
