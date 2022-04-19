package com.wavesenterprise.network

import cats.Show
import com.google.common.cache.CacheBuilder
import com.wavesenterprise.network.InvalidBlockStorageImpl._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

trait InvalidBlockStorage {
  def add(blockId: ByteStr, validationError: ValidationError): Unit

  def find(blockId: ByteStr): Option[ValidationError]
}

class InvalidBlockStorageImpl(settings: InvalidBlockStorageSettings) extends InvalidBlockStorage {
  private val cache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(settings.timeout.length, settings.timeout.unit)
    .build[ByteStr, ValidationError]()

  override def add(blockId: ByteStr, validationError: ValidationError): Unit = {
    validationError match {
      case _: ValidationError.BlockFromFuture => ()
      case error                              => cache.put(blockId, error)
    }
  }

  override def find(blockId: ByteStr): Option[ValidationError] = Option(cache.getIfPresent(blockId))
}

object InvalidBlockStorageImpl {

  case class InvalidBlockStorageSettings(maxSize: Int, timeout: FiniteDuration)

  implicit val configReader: ConfigReader[InvalidBlockStorageSettings] = deriveReader

  implicit val toPrintable: Show[InvalidBlockStorageSettings] = { x =>
    import x._

    s"""
       |maxSize: $maxSize
       |timeout: $timeout
     """.stripMargin
  }
}
