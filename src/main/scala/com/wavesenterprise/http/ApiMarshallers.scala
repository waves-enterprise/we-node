package com.wavesenterprise.http

import akka.http.scaladsl.marshalling.{Marshaller, PredefinedToEntityMarshallers, ToEntityMarshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.MediaTypes.{`application/json`, `text/plain`}
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshaller}
import akka.util.ByteString
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.transaction.{Transaction, ValidationError}
import play.api.libs.json._

import scala.util.control.Exception.nonFatalCatch
import scala.util.control.NoStackTrace

case class JsonException(cause: Option[Throwable] = None, errors: Seq[(JsPath, Seq[JsonValidationError])] = Seq.empty)
    extends IllegalArgumentException
    with NoStackTrace

trait ApiMarshallers {

  import akka.http.scaladsl.marshalling.PredefinedToResponseMarshallers._

  implicit val apiErrorMarshaller: ToResponseMarshaller[ApiError] =
    fromStatusCodeAndValue[StatusCode, JsValue].compose(apiError => apiError.code -> apiError.json)

  implicit val validationErrorMarshaller: ToResponseMarshaller[ValidationError] =
    apiErrorMarshaller.compose(validationError => ApiError.fromValidationError(validationError))

  implicit val transactionWrites: Writes[Transaction] = Writes(_.json())

  protected val jsonStringUnmarshaller: FromEntityUnmarshaller[String] =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(`application/json`)
      .mapWithCharset {
        case (ByteString.empty, _) => throw Unmarshaller.NoContentException
        case (data, charset)       => data.decodeString(charset.nioCharset.name)
      }

  protected lazy val jsonStringMarshaller: ToEntityMarshaller[String] =
    Marshaller.stringMarshaller(`application/json`)

  implicit def playJsonUnmarshaller[A](implicit reads: Reads[A]): FromEntityUnmarshaller[A] =
    jsonStringUnmarshaller map { data =>
      val json = nonFatalCatch.withApply(t => throw JsonException(cause = Some(t)))(Json.parse(data))

      json.validate[A] match {
        case JsSuccess(value, _) => value
        case JsError(errors)     => throw JsonException(errors = errors)
      }
    }

  // preserve support for extracting plain strings from requests
  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] = PredefinedFromEntityUnmarshallers.stringUnmarshaller
  implicit val intUnmarshaller: FromEntityUnmarshaller[Int]       = stringUnmarshaller.map(_.toInt)

  implicit def playJsonMarshaller[A](implicit writes: Writes[A], printer: JsValue => String = Json.prettyPrint): ToEntityMarshaller[A] =
    jsonStringMarshaller.compose(printer).compose(writes.writes)

  // preserve support for using plain strings as request entities
  implicit val stringMarshaller = PredefinedToEntityMarshallers.stringMarshaller(`text/plain`)
}

object ApiMarshallers extends ApiMarshallers
