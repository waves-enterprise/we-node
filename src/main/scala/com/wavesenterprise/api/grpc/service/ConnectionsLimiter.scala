package com.wavesenterprise.api.grpc.service

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.wavesenterprise.api.grpc.service.ConnectionsLimiter.ConnectionId
import com.wavesenterprise.api.grpc.utils.ApiErrorExt
import com.wavesenterprise.api.http.ApiError
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import java.util.concurrent.Semaphore

trait ConnectionsLimiter extends ConnectionIdsHolder {
  def maxConnections: Int
  def scheduler: Scheduler

  private lazy val connectionsSemaphore = new Semaphore(maxConnections)

  def withConnectionsLimiter[T](metadata: Metadata, requestStr: String)(f: ConnectionId => Either[Throwable, Observable[T]]): Source[T, NotUsed] = {
    getConnectionId(metadata) match {
      case Left(err) =>
        log.debug(s"Couldn't retrieve connection-id: ${err.message}")
        Source.failed(err.asGrpcServiceException)
      case Right(connectionId) =>
        log.warn(s"availablePermits=${connectionsSemaphore.availablePermits()}")
        if (connectionsSemaphore.tryAcquire()) {
          log.debug(s"Incoming gRPC connection, unique id '$connectionId', available connections '${connectionsSemaphore.availablePermits()}'")
          log.debug(s"[$connectionId] $requestStr")
          f(connectionId) match {
            case Left(err) =>
              connectionsSemaphore.release()
              connectionIds.remove(connectionId)
              log.warn {
                s"gRPC connection '$connectionId' is closed due to auth problems, available connections '${connectionsSemaphore.availablePermits()}'"
              }
              Source.failed(err)
            case Right(result) =>
              Source
                .fromPublisher {
                  result
                    .guarantee {
                      Task {
                        connectionsSemaphore.release()
                        connectionIds.remove(connectionId)
                        log.debug(s"gRPC connection '$connectionId' closed, available connections '${connectionsSemaphore.availablePermits()}'")
                      }
                    }
                    .toReactivePublisher(scheduler)
                }
                .map(identity) // workaround to pass custom attributes after `fromPublisher` method https://github.com/akka/akka/issues/30076
                .withAttributes(Attributes.inputBuffer(1, 1))
          }
        } else {
          connectionIds.remove(connectionId)
          Source.failed(ApiError.TooManyConnections(maxConnections).asGrpcServiceException)
        }
    }
  }

}

object ConnectionsLimiter {
  type ConnectionId = String
}
