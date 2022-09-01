package com.wavesenterprise.api.grpc.service

import com.wavesenterprise.api.grpc.service.ConnectionsLimiter.ConnectionId
import com.wavesenterprise.api.grpc.utils.ApiErrorExt
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.ScorexLogging
import monix.reactive.Observable
import squants.information.Information

trait HistoryEventsSupport[T] extends ScorexLogging {
  def eventSizeInBytes: Int
  def blockchainUpdater: Blockchain with BlockchainUpdater
  def state: Blockchain with PrivacyState
  def observeFromHeight(startHeight: Int, connectionId: ConnectionId): Observable[T]

  def getEventsBufferSize(bufferSizeInBytes: Information): Int = {
    val bufferSize = (bufferSizeInBytes.toBytes / eventSizeInBytes).toInt
    if (bufferSize < 2) {
      log.warn("Calculated objects buffer size is lower than 2. The default buffer size 2 is being used")
      2
    } else {
      bufferSize
    }
  }

  def observeFromSignature(signature: ByteStr, connectionId: ConnectionId): Observable[T] = {
    state
      .heightOf(signature)
      .fold {
        log.warn(s"Block signature '$signature' not found")
        Observable.raiseError[T](ApiError.BlockDoesNotExist(Left(signature)).asGrpcServiceException)
      } { height =>
        observeFromHeight(height + 1, connectionId)
      }
  }
}
