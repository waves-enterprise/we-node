package com.wavesenterprise.api

import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import cats.data.{NonEmptyList, Validated}
import cats.implicits.catsSyntaxEither
import cats.instances.list.catsStdInstancesForList
import cats.syntax.traverse.toTraverseOps
import com.wavesenterprise.api.http.ApiError
import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

sealed trait ValidLong extends EnumEntry {
  protected def validation: Long => Boolean
  protected def description: String

  protected final def validate(i: Long): Validated[String, Long] = {
    Validated.cond(validation(i), i, s"'$i' must be $description")
  }

  protected final def validateStr(str: String): Validated[String, Long] = {
    Validated
      .catchOnly[NumberFormatException](str.toLong)
      .leftMap(_ => s"Unable to parse Long from '$str'")
      .andThen(validate)
  }
}

object ValidLong extends Enum[ValidLong] {
  override val values: immutable.IndexedSeq[ValidLong] = findValues

  case object PositiveLong extends ValidLong {
    override protected val validation: Long => Boolean = _ > 0
    override protected val description: String         = "positive"

    def apply(str: String): Validated[String, Long] = validateStr(str)
    def apply(i: Long): Validated[String, Long]     = validate(i)
  }

  case object NonNegativeLong extends ValidLong {
    override protected val validation: Long => Boolean = _ >= 0
    override protected val description: String         = "non-negative"

    def apply(str: String): Validated[String, Long] = validateStr(str)
    def apply(i: Long): Validated[String, Long]     = validate(i)
  }

  implicit class ValidatedLongListExt(private val v: List[Validated[String, Long]]) extends AnyVal {
    def toApiError: Either[ApiError, List[Long]] = {
      v.traverse(_.leftMap(NonEmptyList.of(_))).toEither.leftMap { errors =>
        ApiError.CustomValidationError(s"Invalid parameters: [${errors.toList.mkString(", ")}]")
      }
    }

    def processRoute(f: List[Long] => StandardRoute): StandardRoute = {
      v.toApiError match {
        case Right(validLongs) => f(validLongs)
        case Left(apiError)    => complete(apiError)
      }
    }
  }

  implicit class ValidatedLongExt(private val v: Validated[String, Long]) extends AnyVal {
    def toApiError[T]: Either[ApiError, Long] = {
      v.toEither.leftMap(err => ApiError.CustomValidationError(s"Invalid parameter: $err"))
    }

    def processRoute(f: Long => StandardRoute): StandardRoute = {
      v.toApiError match {
        case Right(validLong) => f(validLong)
        case Left(apiError)   => complete(apiError)
      }
    }
  }
}
