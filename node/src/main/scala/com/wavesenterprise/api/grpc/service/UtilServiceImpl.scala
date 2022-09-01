package com.wavesenterprise.api.grpc.service

import akka.grpc.scaladsl.Metadata
import com.google.protobuf.empty.Empty
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.protobuf.service.util.{NodeTimeResponse, UtilPublicServicePowerApi}
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class UtilServiceImpl(val authSettings: AuthorizationSettings, val nodeOwner: Address, val time: Time)(implicit s: Scheduler)
    extends UtilPublicServicePowerApi
    with GrpcAuth {
  override def getNodeTime(in: Empty, metadata: Metadata): Future[NodeTimeResponse] =
    withAuthTask(metadata) {
      Task {
        NodeTimeResponse(System.currentTimeMillis(), time.correctedTime())
      }
    }.runToFuture
}
