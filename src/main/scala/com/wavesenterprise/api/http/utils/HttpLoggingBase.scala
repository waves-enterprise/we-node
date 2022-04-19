package com.wavesenterprise.api.http.utils

import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import com.wavesenterprise.utils.ScorexLogging

trait HttpLoggingBase extends ScorexLogging {

  @inline
  def logRequest(request: HttpRequest): Long = {
    log.trace(s"${request.protocol.value} request from '${request.method.value} ${request.uri}'")
    System.currentTimeMillis()
  }

  @inline
  def logResponse(request: HttpRequest, response: HttpResponse, start: Long): HttpResponse = {
    val took = System.currentTimeMillis() - start
    val msg  = s"${request.protocol.value} response ${response.status.value} for ${request.method.value} ${request.uri} (took ${took}ms)"
    if (response.status == StatusCodes.OK) log.debug(msg) else log.warn(msg)
    response
  }
}
