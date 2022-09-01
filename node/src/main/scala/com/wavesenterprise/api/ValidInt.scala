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

sealed trait ValidInt extends EnumEntry {
  protected def validation: Int => Boolean
  protected def description: String

  protected final def validate(i: Int): Validated[String, Int] = {
    Validated.cond(validation(i), i, s"'$i' must be $description")
  }

  protected final def validateStr(str: String): Validated[String, Int] = {
    Validated
      .catchOnly[NumberFormatException](str.toInt)
      .leftMap(_ => s"Unable to parse Integer from '$str'")
      .andThen(validate)
  }
}

object ValidInt extends Enum[ValidInt] {
  override val values: immutable.IndexedSeq[ValidInt] = findValues

  case object PositiveInt extends ValidInt {
    override protected val validation: Int => Boolean = _ > 0
    override protected val description: String        = "positive"

    def apply(str: String): Validated[String, Int] = validateStr(str)
    def apply(i: Int): Validated[String, Int]      = validate(i)
  }

  case object NonNegativeInt extends ValidInt {
    override protected val validation: Int => Boolean = _ >= 0
    override protected val description: String        = "non-negative"

    def apply(str: String): Validated[String, Int] = validateStr(str)
    def apply(i: Int): Validated[String, Int]      = validate(i)
  }

  implicit class ValidatedIntListExt(private val v: List[Validated[String, Int]]) extends AnyVal {
    def toApiError: Either[ApiError, List[Int]] = {
      v.traverse(_.leftMap(NonEmptyList.of(_))).toEither.leftMap { errors =>
        ApiError.CustomValidationError(s"Invalid parameters: [${errors.toList.mkString(", ")}]")
      }
    }

    def processRoute(f: List[Int] => StandardRoute): StandardRoute = {
      v.toApiError match {
        case Right(validInts) => f(validInts)
        case Left(apiError)   => complete(apiError)
      }
    }
  }

  implicit class ValidatedIntExt(private val v: Validated[String, Int]) extends AnyVal {
    def toApiError[T]: Either[ApiError, Int] = {
      v.toEither.leftMap(err => ApiError.CustomValidationError(s"Invalid parameter: $err"))
    }

    def processRoute(f: Int => StandardRoute): StandardRoute = {
      v.toApiError match {
        case Right(validInt) => f(validInt)
        case Left(apiError)  => complete(apiError)
      }
    }
  }
}
