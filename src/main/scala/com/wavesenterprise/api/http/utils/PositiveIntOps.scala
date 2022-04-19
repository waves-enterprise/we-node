package com.wavesenterprise.api.http.utils

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import cats.data.ValidatedNel
import com.wavesenterprise.settings.PositiveInt

object PositiveIntOps {
  implicit class processValNel(private val x: ValidatedNel[String, List[PositiveInt]]) extends AnyVal {
    def process(f: List[Int] => StandardRoute): StandardRoute =
      x.fold(
        errors => complete(HttpResponse(BadRequest, entity = s"Path components: [${errors.toList.mkString("; ")}] must be positive integers")),
        positiveInts => f(positiveInts.map(_.value))
      )
  }
}
