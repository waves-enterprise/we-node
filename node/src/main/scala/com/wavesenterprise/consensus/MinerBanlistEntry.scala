package com.wavesenterprise.consensus

import cats.Show
import cats.syntax.either._
import com.google.common.io.ByteArrayDataInput
import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError

sealed trait MinerBanlistEntry extends Product with Serializable { self =>
  val timestamp: Long
  def bytes: Array[Byte]
}

object MinerBanlistEntry {
  case class CancelledWarning(cancellationTimestamp: Long, warning: Warning)

  case class Warning(timestamp: Long) extends MinerBanlistEntry {
    override def bytes: Array[Byte] = {
      val out = newDataOutput()
      out.writeByte(Warning.typeByte)
      out.writeLong(timestamp)
      out.toByteArray
    }

    override def toString: String = s"Warning(timestamp: $timestamp)"
  }
  object Warning { val typeByte: Byte = 1 }

  case class Ban(timestamp: Long, beginHeight: Int, priorWarningTimestamps: PriorWarningsInfo) extends MinerBanlistEntry {
    override def bytes: Array[Byte] = {
      val out = newDataOutput()
      out.writeByte(Ban.typeByte)
      out.writeLong(timestamp)
      out.writeInt(beginHeight)
      out.writeShort(priorWarningTimestamps.timestamps.length.toShort)
      priorWarningTimestamps.timestamps.foreach(out.writeLong)
      out.toByteArray
    }

    def endHeight(banDurationBlocks: Int): Int = beginHeight + banDurationBlocks

    override def toString: String = s"Ban(timestamp: $timestamp, beginHeight: $beginHeight, priorWarnings: $priorWarningTimestamps"
  }
  object Ban { val typeByte: Byte = 2 }

  private[consensus] case class PriorWarningTimestamps(timestamp1: Long, timestamp2: Long) {
    def toWarnings: List[Warning] =
      Warning(timestamp1) :: Warning(timestamp2) :: Nil
  }

  private[consensus] case class PriorWarningsInfo(timestamps: Array[Long]) {
    def toWarnings: Seq[Warning] =
      timestamps.map(Warning.apply)

    override def toString: String = s"PriorWarnings(${timestamps.mkString(", ")})"

    override def equals(obj: Any): Boolean = obj match {
      case PriorWarningsInfo(otherTimestamps) => timestamps.sameElements(otherTimestamps)
      case _                                  => false
    }
  }

  def fromDataInput(input: ByteArrayDataInput): Either[ValidationError, MinerBanlistEntry] = {
    Either
      .catchNonFatal(input.readByte())
      .flatMap {
        case Warning.typeByte =>
          Either
            .catchNonFatal(input.readLong())
            .map(Warning.apply)

        case Ban.typeByte =>
          Either
            .catchNonFatal {
              val parsedTimestamp = input.readLong()
              val parsedHeight    = input.readInt()
              val timestampsCount = input.readShort()
              val warningTimestamps = (0 until timestampsCount).toArray.map { _ =>
                input.readLong()
              }
              Ban(parsedTimestamp, parsedHeight, PriorWarningsInfo(warningTimestamps))
            }

        case otherByte =>
          Left(new Exception(s"Cannot parse MinerBanlistEntry: unknown entry type byte '$otherByte'"))
      }
      .leftMap(ex => GenericError(s"Cannot parse MinerBanlistEntry: ${ex.getMessage}"))
  }

  def fromBytes(bytes: Array[Byte]): Either[ValidationError, MinerBanlistEntry] = {
    val input = newDataInput(bytes)
    fromDataInput(input)
  }

  implicit val toPrintable: Show[MinerBanlistEntry] = { _.toString }
}
