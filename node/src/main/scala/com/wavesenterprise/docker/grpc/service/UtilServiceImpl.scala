package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import com.google.protobuf.empty.Empty
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.protobuf.service.contract.{NodeTimeResponse, UtilServicePowerApi}
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class UtilServiceImpl(val time: Time, val contractAuthTokenService: ContractAuthTokenService, val scheduler: Scheduler)
    extends UtilServicePowerApi
    with WithServiceAuth {

  override def getNodeTime(request: Empty, metadata: Metadata): Future[NodeTimeResponse] =
    withRequestAuth(metadata,
                    Task {
                      NodeTimeResponse(System.currentTimeMillis(), time.correctedTime())
                    }).runToFuture(scheduler)
}
