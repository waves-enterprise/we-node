package com.wavesenterprise.http

import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.testkit.RouteTest
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.http.ApiMarshallers._
import org.scalatest.matchers.{MatchResult, Matcher}
import play.api.libs.json._

trait ApiErrorMatchers { this: RouteTest =>
  class ProduceError(statusCode: StatusCode, json: JsObject) extends Matcher[RouteTestResult] {
    override def apply(left: RouteTestResult): MatchResult = left ~> check {
      if (response.status != statusCode) {
        MatchResult(false,
                    "got {0} while expecting {1}, response was {2}",
                    "got expected status code {0}",
                    IndexedSeq(response.status, statusCode, response.entity))
      } else {
        val responseJson = responseAs[JsObject]
        MatchResult(responseJson == json,
                    "expected {0}, but instead got {1}",
                    "expected not to get {0}, but instead did get it",
                    IndexedSeq(json, responseJson))
      }
    }
  }

  object ProduceError {
    def apply(error: ApiError) = new ProduceError(error.code, error.json)
  }

  def produce(error: ApiError)                        = ProduceError(error)
  def produce(statusCode: StatusCode, json: JsObject) = new ProduceError(statusCode, json)
}
