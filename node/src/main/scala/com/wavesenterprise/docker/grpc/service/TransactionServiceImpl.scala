package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import com.wavesenterprise.api.grpc.base.TransactionServiceBase
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.docker.{ContractAuthTokenService, deferEither}
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.protobuf.service.util.{TransactionInfoRequest, TransactionInfoResponse}
import monix.execution.Scheduler

import scala.concurrent.Future

class TransactionServiceImpl(val blockchain: Blockchain, val contractAuthTokenService: ContractAuthTokenService, val scheduler: Scheduler)
    extends TransactionServiceBase
    with TransactionServicePowerApi
    with WithServiceAuth {

  override def transactionExists(request: TransactionExistsRequest, metadata: Metadata): Future[TransactionExistsResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        for {
          txId <- parseTxId(request.txId)
          exists = blockchain.transactionInfo(txId).isDefined
        } yield TransactionExistsResponse(exists)
      }
    ).runToFuture(scheduler)

  override def transactionInfo(request: TransactionInfoRequest, metadata: Metadata): Future[TransactionInfoResponse] =
    withRequestAuth(
      metadata,
      deferEither(transactionInfo(request.txId))
    ).runToFuture(scheduler)
}
