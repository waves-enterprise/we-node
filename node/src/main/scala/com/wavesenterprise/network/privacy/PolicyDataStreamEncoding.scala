package com.wavesenterprise.network.privacy

import enumeratum.values.{ByteEnum, ByteEnumEntry}

import scala.collection.immutable

object PolicyDataStreamEncoding {

  sealed abstract class PolicyDataStreamResponse(override val value: Byte) extends ByteEnumEntry

  object PolicyDataStreamResponse extends ByteEnum[PolicyDataStreamResponse] {

    case object DataNotFound    extends PolicyDataStreamResponse(0)
    case object HasData         extends PolicyDataStreamResponse(1)
    case object TooManyRequests extends PolicyDataStreamResponse(2)

    override def values: immutable.IndexedSeq[PolicyDataStreamResponse] = findValues
  }
}
