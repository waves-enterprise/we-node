package com.wavesenterprise.utils

import cats.data.EitherT
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import monix.eval.Task
import squants.information.Kilobytes

import java.nio.file.{Files, Path}
import java.util.zip.CRC32

object ZipUtils {
  private val ReadBufferSize              = Kilobytes(128).toBytes.toInt
  private val MaxCommentLength            = Kilobytes(64).toBytes.toInt
  private val MinEOCDLength               = 22
  private val EOCDCommentPosition         = 20
  private val MaxEOCDLength               = MinEOCDLength + MaxCommentLength
  private val StartOECDBytes: Array[Byte] = Array(0x50, 0x4b, 0x05, 0x06)

  def getFileChecksum(file: Path, bytesLimit: Option[Long] = None): Task[Array[Byte]] = {
    Task(Files.newInputStream(file))
      .bracket { is =>
        Task {
          val crc = new CRC32()
          @annotation.tailrec
          def getCrcChecksum(remainingBytes: Option[Long]): Array[Byte] = {
            if (remainingBytes.exists(_ <= 0)) {
              BigInt(crc.getValue).toByteArray
            } else {
              val toRead = remainingBytes.fold(ReadBufferSize)(math.min(ReadBufferSize, _).toInt)
              val batch  = is.readNBytes(toRead)
              if (batch.isEmpty) {
                BigInt(crc.getValue).toByteArray
              } else {
                crc.update(batch)
                getCrcChecksum(remainingBytes.map(_ - batch.length))
              }
            }
          }
          getCrcChecksum(bytesLimit)
        }
      } { is =>
        Task(is.close())
      }
  }

  /**
    * @return EOCD position with comment at the same time to avoid file's double traverse
    */
  def getEOCDInfo(path: Path): Task[Either[ValidationError, EOCDInfo]] = {
    (for {
      fileSize <- EitherT.right(Task(path.toFile.length()))
      _        <- EitherT.cond[Task](fileSize >= MinEOCDLength, (), GenericError(s"Unexpected zip archive size: ${fileSize}B"))
      bufferSize = math.min(fileSize, MaxEOCDLength).toInt
      offset     = fileSize - bufferSize
      bytes        <- EitherT.right(readNBytes(path, bufferSize, offset))
      eocdPosition <- EitherT(Task(findEOCDBytesPosition(bytes, bufferSize - MinEOCDLength)))
      zipComment   <- EitherT.right[ValidationError](Task(parseZipComment(bytes, eocdPosition)))
    } yield EOCDInfo(offset + eocdPosition, zipComment)).value
  }

  private def parseZipComment(bytes: Array[Byte], eocdPosition: Int): String = {
    val commentLengthStart = eocdPosition + EOCDCommentPosition
    val commentLengthEnd   = commentLengthStart + 2
    val sizeBytes          = bytes.slice(commentLengthStart, commentLengthEnd)
    // built-in java methods don't work here
    val commentLength = sizeBytes.head & 0xff | ((sizeBytes.last & 0xff) << 8)
    new String(bytes.slice(commentLengthEnd, commentLengthEnd + commentLength))
  }

  private def readNBytes(file: Path, n: Int, offset: Long): Task[Array[Byte]] = {
    Task(Files.newInputStream(file)).bracket { is =>
      Task {
        is.skip(offset)
        is.readNBytes(n)
      }
    } { is =>
      Task(is.close())
    }
  }

  /**
    * Traverses bytes in reverse order because {@link java.util.zip.ZipOutputStream#setComment(String)} creates additional EOCD section
    */
  @annotation.tailrec
  private def findEOCDBytesPosition(bytes: Array[Byte], currPos: Int): Either[ValidationError, Int] = {
    if (currPos < 0) {
      Left(GenericError("No EOCD bytes found"))
    } else if (bytes.slice(currPos, currPos + 4) sameElements StartOECDBytes) {
      Right(currPos)
    } else {
      findEOCDBytesPosition(bytes, currPos - 1)
    }
  }

  case class EOCDInfo(startPosition: Long, comment: String)
}
