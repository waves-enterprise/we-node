package com.wavesenterprise.settings

import java.time.{Duration => JDuration}
import akka.http.scaladsl.model.Uri
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.validated._
import cats.instances.list.catsStdInstancesForList
import cats.syntax.traverse.toTraverseOps

import com.typesafe.config.Config
import com.typesafe.config.ConfigValueType._
import com.wavesenterprise.account.Address
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ScorexLogging
import net.ceedubs.ficus.readers.ValueReader
import net.ceedubs.ficus.{FicusConfig, FicusInstances, SimpleFicusConfig}
import okhttp3.HttpUrl
import pureconfig.ConfigReader
import pureconfig.ConvertHelpers.catchReadError
import pureconfig.error.{CannotConvert, WrongType}
import software.amazon.awssdk.regions.Region

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

case class PositiveInt(value: Int) {
  require(value > 0)
}

object PositiveInt {
  private val stringToPositiveInt: String => ValidatedNel[String, PositiveInt] = s =>
    Validated
      .catchOnly[NumberFormatException](s.toInt)
      .leftMap(_ => NonEmptyList.of(s))
      .andThen {
        case i if i > 0 => PositiveInt(i).valid
        case negative   => NonEmptyList.of(negative.toString).invalid
    }

  def fromStr(keys: String*): ValidatedNel[String, List[PositiveInt]] = keys.toList.traverse(stringToPositiveInt)
}

sealed trait PureConfigReaders {
  implicit val rawConfigReader: ConfigReader[Config]       = ConfigReader.fromCursor(_.asObjectCursor.map(_.value.toConfig))
  implicit val longConfigReader: ConfigReader[Long]        = ConfigReader.fromNonEmptyString[Long](catchReadError(_.toLong))
  implicit val intConfigReader: ConfigReader[Int]          = ConfigReader.fromNonEmptyString[Int](catchReadError(_.toInt))
  implicit val byteConfigReader: ConfigReader[Byte]        = ConfigReader.fromNonEmptyString[Byte](catchReadError(_.toByte))
  implicit val shortConfigReader: ConfigReader[Short]      = ConfigReader.fromNonEmptyString[Short](catchReadError(_.toShort))
  implicit val uriConfigReader: ConfigReader[Uri]          = ConfigReader.fromNonEmptyString[Uri](catchReadError(Uri(_)))
  implicit val httpUrlConfigReader: ConfigReader[HttpUrl]  = ConfigReader.fromNonEmptyString[HttpUrl](catchReadError(HttpUrl.parse))
  implicit val awsRegionConfigReader: ConfigReader[Region] = ConfigReader.fromNonEmptyString[Region](catchReadError(Region.of))
  implicit val javaDurationConfigReader: ConfigReader[JDuration] = ConfigReader.fromNonEmptyString[JDuration] {
    catchReadError(str => JDuration.ofMillis(Duration(str).toMillis))
  }
  implicit val westAmountConfigReader: ConfigReader[WestAmount] = ConfigReader.fromNonEmptyString[WestAmount] {
    catchReadError { str =>
      WestAmount
        .fromString(str)
        .fold(err => throw new IllegalArgumentException(err.toString), identity)
    }
  }

  def emptyMapReader[K, V](implicit reader: ConfigReader[Map[K, V]]): ConfigReader[Map[K, V]] =
    ConfigReader.fromCursor[Map[K, V]] { cv =>
      if (cv.isNull || cv.asString.exists(_.isEmpty))
        Right(Map.empty[K, V])
      else
        reader.from(cv)
    }

  implicit val positiveIntReader: ConfigReader[PositiveInt] = ConfigReader.fromCursor[PositiveInt] { cursor =>
    cursor.asString.flatMap { str =>
      Try(str.toInt) match {
        case Success(n) if n > 0 => Right(PositiveInt(n))
        case Success(n)          => cursor.failed(CannotConvert(str, "PositiveInt", s"'$n' is not positive"))
        case Failure(_)          => cursor.failed(WrongType(STRING, Set(NUMBER)))
      }
    }
  }
}

trait WEConfigReaders extends FicusInstances with PureConfigReaders with ScorexLogging {

  override implicit val intValueReader: ValueReader[Int] = { (cfg: Config, path: String) =>
    val strValue = cfg.getString(path)
    Try(strValue.toInt) match {
      case Failure(_) =>
        val message = errorMsg(path, strValue)
        log.error(message)
        throw new IllegalArgumentException(message)
      case Success(intValue) => intValue
    }
  }

  override implicit val longValueReader: ValueReader[Long] = { (cfg: Config, path: String) =>
    val strValue = cfg.getString(path)
    Try(strValue.toLong) match {
      case Failure(_) =>
        val message = errorMsg(path, strValue)
        log.error(message)
        throw new IllegalArgumentException(message)
      case Success(longValue) => longValue
    }
  }

  implicit val blockchainAddressConfigReader: ValueReader[Address] = (cfg: Config, path: String) => {
    val valueFromConfig   = cfg.getString(path)
    val addressFromConfig = Address.fromString(valueFromConfig)
    addressFromConfig match {
      case Left(err) =>
        log.error(s"Failed to parse Address from config. Config path: '$path', config value: '$valueFromConfig', validation error: '$err'")
      case _ => ()
    }
    addressFromConfig.explicitGet()
  }

  implicit val httpUrlReader: ValueReader[HttpUrl] = (cfg: Config, path: String) => {
    val strValue = cfg.getString(path)
    Option(HttpUrl.parse(strValue)).getOrElse(throw new RuntimeException(s"Invalid HTTP URL: $strValue"))
  }

  implicit val uriReader: ValueReader[Uri] = (cfg: Config, path: String) => {
    val strValue = cfg.getString(path)
    Try(Uri(strValue)) match {
      case Success(uri) => uri
      case Failure(ex)  => throw new RuntimeException(s"Invalid URI string: '$strValue'", ex)
    }
  }

  private def errorMsg(cfgPath: String, invalidValue: String): String = {
    s"Config parse fail: value by path '$cfgPath' have invalid value '$invalidValue'."
  }
}

object WEConfigReaders extends WEConfigReaders {
  implicit def toFicusConfig(config: Config): FicusConfig = SimpleFicusConfig(config)
}
