package com.wavesenterprise.settings

import enumeratum.Enum
import enumeratum.EnumEntry.Lowercase
import enumeratum.values.ByteEnumEntry
import pureconfig.ConfigConvert.catchReadError
import pureconfig.ConfigReader

import scala.collection.immutable

sealed abstract class NodeMode(val value: Byte) extends ByteEnumEntry with Lowercase

object NodeMode extends Enum[NodeMode] {

  case object Default extends NodeMode(0)
  case object Watcher extends NodeMode(1)

  override val values: immutable.IndexedSeq[NodeMode] = findValues

  private val valuesByByte: Map[Byte, NodeMode] = values.map(m => m.value -> m).toMap

  def withValue(b: Byte): Either[String, NodeMode] = {
    valuesByByte.get(b).toRight(s"$b is not a member of Enum ($values)")
  }

  implicit val configReader: ConfigReader[NodeMode] = ConfigReader.fromNonEmptyString[NodeMode](catchReadError(withNameInsensitive))
}
