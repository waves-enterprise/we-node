package com.wavesenterprise.generator.common

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import cats.data.EitherT
import com.wavesenterprise.api.http.{ApiErrorResponse => OriginalApiErrorResponse}
import com.wavesenterprise.generator.common.RestTooling._
import com.wavesenterprise.http.ApiMarshallers.playJsonUnmarshaller
import com.wavesenterprise.transaction.{Transaction, TransactionFactory}
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import play.api.libs.json._

import scala.concurrent.duration.FiniteDuration

trait RestTooling { self: ScorexLogging =>
  def httpSender: HttpExt
  implicit val system: ActorSystem
  val RequestTimeout: FiniteDuration

  /**
    * Send a HttpRequest
    * May result in expected value, ApiErrorResponse or a Task failure
    */
  protected def sendRequest[R](req: HttpRequest, timeout: FiniteDuration)(implicit um: Unmarshaller[HttpResponse, R]): EitherT[Task, ApiError, R] =
    EitherT {
      Task
        .deferFuture(httpSender.singleRequest(req))
        .timeout(timeout)
        .flatMap { rawResponse =>
          parseToResponseOrError(rawResponse, um).value
        }
    }

  /**
    * Try parsing a response to expected class, on fail attempt to parse it into ApiErrorResponse
    */
  protected def parseToResponseOrError[R](rawResponse: HttpResponse,
                                          responseUnmarshaller: Unmarshaller[HttpResponse, R]): EitherT[Task, ApiError, R] =
    EitherT {
      val apiErrorUnmarshaller         = implicitly[Unmarshaller[HttpResponse, ApiError]]
      val originalApiErrorUnmarshaller = implicitly[Unmarshaller[HttpResponse, OriginalApiErrorResponse]]

      parseResponseTask(rawResponse)(responseUnmarshaller).attempt.flatMap {
        case Right(parsedResponse) =>
          Task.now(Right(parsedResponse))

        case Left(_) =>
          parseResponseTask(rawResponse)(apiErrorUnmarshaller)
            .map(apiError => Left(apiError))
            .onErrorFallbackTo {
              parseResponseTask(rawResponse)(originalApiErrorUnmarshaller)
                .flatMap { apiError =>
                  log.error(s"Encountered ApiError: '$apiError'")
                  Task.raiseError(new RuntimeException(s"Received ApiError response"))
                }
            }
      }
    }

  /**
    * Strictly parse a response
    */
  protected def parseResponseTask[R](rawResponse: HttpResponse)(implicit um: Unmarshaller[HttpResponse, R]): Task[R] = {
    Task.deferFuture(Unmarshal(rawResponse).to[R])
  }
}

object RestTooling {
  case class TxIdResponse(id: String)
  case class MinedTxIdResponse(id: String, height: Int)
  case class ApiError(status: String, details: String) {
    override def toString: String = s"ApiError with status code: '$status', details: '$details'"
  }

  implicit val broadcastResponseFormat: Format[TxIdResponse]              = Json.format
  implicit val apiErrorsFormat: Format[ApiError]                          = Json.format
  implicit val originalApiErrorResponse: Format[OriginalApiErrorResponse] = Json.format
  implicit val minedTxResponseFormat: Format[MinedTxIdResponse]           = Json.format
  implicit val signedTxResponseReads: Reads[Transaction] = Reads { jsv =>
    TransactionFactory.fromSignedRequest(jsv) match {
      case Right(tx) => JsSuccess(tx)
      case Left(err) => JsError(err.toString)
    }
  }
}
