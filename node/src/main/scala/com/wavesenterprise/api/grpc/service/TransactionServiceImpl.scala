package com.wavesenterprise.api.grpc.service

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import cats.data.EitherT
import cats.syntax.either.catsSyntaxEither
import com.google.protobuf.empty.Empty
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.grpc.base.TransactionServiceBase
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.{IllegalWatcherActionError, InvalidSignature, UnimplementedError}
import com.wavesenterprise.api.{TransactionsApi, parseCertChain}
import com.wavesenterprise.network.TxBroadcaster
import com.wavesenterprise.privacy.PolicyStorage
import com.wavesenterprise.protobuf.service.transaction.{TransactionPublicServicePowerApi, UtxSize}
import com.wavesenterprise.protobuf.service.util.{
  TransactionInfoRequest,
  TransactionInfoResponse,
  TransactionWithCertChain => PbTransactionWithCertChain
}
import com.wavesenterprise.settings.{AuthorizationSettings, NodeMode}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.protobuf.{Transaction => PbTransaction}
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.transaction.{ProtoSerializableTransaction, TransactionFactory, ValidationError}
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class TransactionServiceImpl(
    val blockchain: Blockchain,
    val feeCalculator: FeeCalculator,
    val txBroadcaster: TxBroadcaster,
    val policyStorage: PolicyStorage,
    val authSettings: AuthorizationSettings,
    val nodeOwner: Address,
    val time: Time,
    val nodeMode: NodeMode
)(implicit scheduler: Scheduler)
    extends TransactionServiceBase
    with TransactionPublicServicePowerApi
    with TransactionsApi
    with GrpcAuth {

  override def broadcast(in: PbTransaction, metadata: Metadata): Future[PbTransaction] = {
    withAuthTask(metadata) {
      (for {
        _  <- EitherT.cond[Task](nodeMode == NodeMode.Default, (), IllegalWatcherActionError)
        tx <- EitherT.fromEither[Task](TransactionFactory.fromProto(in)).leftMap(ApiError.fromValidationError)
        _  <- validateAndBroadcastTransaction(tx)
      } yield tx.toProto).value
    }.flatMap(_.asTask).runToFuture
  }

  override def broadcastWithCerts(in: PbTransactionWithCertChain, metadata: Metadata): Future[PbTransaction] = {
    withAuthTask(metadata) {
      (for {
        _              <- EitherT.cond[Task](nodeMode == NodeMode.Default, (), IllegalWatcherActionError)
        maybeCertChain <- EitherT.fromEither[Task](parseCertChain(in.certificates.map(_.toByteArray).toList).leftMap(ApiError.fromValidationError))
        tx <- EitherT.fromEither[Task] {
          in.transaction
            .toRight(ValidationError.GenericError("Missing 'transaction' field"))
            .flatMap(TransactionFactory.fromProto)
            .leftMap(ApiError.fromValidationError)
        }
        _ <- validateAndBroadcastTransaction(tx, maybeCertChain)
      } yield tx.toProto).value
    }.flatMap(_.asTask).runToFuture
  }

  override def utxInfo(in: Empty, metadata: Metadata): Source[UtxSize, NotUsed] =
    withAuth(metadata)(txBroadcaster.utx.lastSize) match {
      case Left(err)  => Source.failed(err)
      case Right(obs) => Source.fromPublisher(obs.toReactivePublisher)
    }

  override def transactionInfo(request: TransactionInfoRequest, metadata: Metadata): Future[TransactionInfoResponse] =
    withAuthTask(metadata) {
      Task.fromEither {
        transactionInfo(request.txId)
      }
    }.runToFuture

  override def unconfirmedTransactionInfo(in: TransactionInfoRequest, metadata: Metadata): Future[PbTransaction] =
    withAuthTask(metadata) {
      Task.fromEither {
        ByteStr
          .decodeBase58(in.txId)
          .toEither
          .leftMap { _ =>
            InvalidSignature.asGrpcServiceException
          }
          .flatMap { id =>
            txBroadcaster.utx.transactionById(id).toRight(ValidationError.TransactionNotFound(id).asGrpcServiceException).flatMap {
              case tx: ProtoSerializableTransaction => Right(tx.toProto)
              case _                                => Left(UnimplementedError("Protobuf serialization not available for this type of transaction").asGrpcServiceException)
            }
          }
      }
    }.runToFuture
}
