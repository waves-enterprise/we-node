package com.wavesenterprise.docker.grpc.service

import akka.NotUsed
import akka.actor.ActorSystem
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import cats.implicits._
import com.wavesenterprise.api.grpc.utils.{parseTxId, _}
import com.wavesenterprise.api.http.service.ContractsApiService
import com.wavesenterprise.crypto
import com.wavesenterprise.docker.grpc.GrpcContractExecutor.ConnectionId
import com.wavesenterprise.docker.grpc.{GrpcContractExecutor, ProtoObjectsMapper}
import com.wavesenterprise.docker.{ContractAuthTokenService, deferEither}
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.utils.Base58
import io.grpc.Status
import monix.eval.Task
import monix.execution.Scheduler
import org.reactivestreams.Publisher

import scala.concurrent.Future
import scala.util.{Failure, Success}

class ContractServiceImpl(
    val grpcContractExecutor: GrpcContractExecutor,
    val contractsApiService: ContractsApiService,
    val contractAuthTokenService: ContractAuthTokenService,
    val scheduler: Scheduler
)(implicit val actorSystem: ActorSystem)
    extends ContractServicePowerApi
    with WithServiceAuth {

  private[this] val bufferSize: Int = 512

  override def connect(request: ConnectionRequest, metadata: Metadata): Source[ContractTransactionResponse, NotUsed] = {
    val connectionId = ConnectionId(request.connectionId)
    (for {
      _ <- checkConnectionRequestAuthorized(metadata).leftMap(Source.failed)
      (sourceQueue, publisher) = createTransactionStream(connectionId)
      _ <- grpcContractExecutor.addConnection(connectionId, sourceQueue).leftMap { ex =>
        log.debug(s"Can't add new connection with id '${connectionId.value}'", ex)
        Source.failed(ex.asGrpcServiceException(Status.INVALID_ARGUMENT))
      }
      _      = log.debug(s"New source was added for connection '${connectionId.value}'")
      source = Source.fromPublisher(publisher)
    } yield source).fold(identity, identity)
  }

  private def createTransactionStream(
      connectionId: ConnectionId): (SourceQueueWithComplete[ContractTransactionResponse], Publisher[ContractTransactionResponse]) = {
    val (sourceQueue, publisher) = Source
      .queue[ContractTransactionResponse](bufferSize, OverflowStrategy.backpressure)
      .toMat(Sink.asPublisher(fanout = true))(Keep.both)
      .run()
    sourceQueue
      .watchCompletion()
      .onComplete {
        case Success(_) =>
          log.info("Transaction stream is completed")
          grpcContractExecutor.removeConnection(connectionId)
        case Failure(e) =>
          log.error("Transaction stream is closed due to error, shutting down the container", e)
          grpcContractExecutor.removeConnection(connectionId)
      }(actorSystem.dispatcher)
    (sourceQueue, publisher)
  }

  override def commitExecutionSuccess(request: ExecutionSuccessRequest, metadata: Metadata): Future[CommitExecutionResponse] =
    deferEither(
      for {
        claim <- checkRequestAuthorized(metadata)
        executionId = claim.executionId
        txId            <- parseTxId(request.txId)
        results         <- request.results.toList.traverse(ProtoObjectsMapper.mapFromProto)
        assetOperations <- request.assetOperations.toList.traverse(ProtoObjectsMapper.mapFromProto)
        _ <- grpcContractExecutor
          .commitExecutionResults(executionId, txId, results, assetOperations)
          .leftMap(ex => ex.asGrpcServiceException(Status.INVALID_ARGUMENT))
      } yield CommitExecutionResponse()
    ).runToFuture(scheduler)

  override def commitExecutionError(request: ExecutionErrorRequest, metadata: Metadata): Future[CommitExecutionResponse] =
    deferEither(
      for {
        claim <- checkRequestAuthorized(metadata)
        executionId = claim.executionId
        txId <- parseTxId(request.txId)
        _ <- grpcContractExecutor
          .commitExecutionError(executionId, txId, request.message, request.code)
          .leftMap(ex => ex.asGrpcServiceException(Status.INVALID_ARGUMENT))
      } yield CommitExecutionResponse()
    ).runToFuture(scheduler)

  override def getContractKeys(request: ContractKeysRequest, metadata: Metadata): Future[ContractKeysResponse] =
    withRequestAuth(
      metadata,
      claim =>
        deferEither {
          for {
            values <- getContractKeysInner(claim.txId, request)
            protoValues = values.map(ProtoObjectsMapper.mapToProto)
          } yield ContractKeysResponse(protoValues)
        }
    ).runToFuture(scheduler)

  private def getContractKeysInner(executableTxId: ByteStr, request: ContractKeysRequest): Either[GrpcServiceException, Vector[DataEntry[_]]] = {
    val readingContext = ContractReadingContext.TransactionExecution(executableTxId)
    (request.matches match {
      case Some(regex) => contractsApiService.contractKeys(request.contractId, request.offset, request.limit, Some(regex), readingContext)
      case None =>
        request.keysFilter match {
          case Some(KeysFilter(keys, _)) => contractsApiService.contractKeys(request.contractId, keys, readingContext)
          case None                      => contractsApiService.contractKeys(request.contractId, request.offset, request.limit, None, readingContext)
        }
    }).leftMap(_.asGrpcServiceException)
  }

  override def getContractKey(request: ContractKeyRequest, metadata: Metadata): Future[ContractKeyResponse] =
    withRequestAuth(
      metadata,
      claim =>
        deferEither {
          val readingContext = ContractReadingContext.TransactionExecution(claim.txId)

          for {
            value <- contractsApiService
              .contractKey(request.contractId, request.key, readingContext)
              .leftMap(_.asGrpcServiceException)
            protoValue = ProtoObjectsMapper.mapToProto(value)
          } yield ContractKeyResponse(Some(protoValue))
        }
    ).runToFuture(scheduler)

  override def getContractBalances(in: ContractBalancesRequest, metadata: Metadata): Future[ContractBalancesResponse] = {
    import cats.implicits._

    withRequestAuth(
      metadata,
      claim =>
        deferEither {
          val balancesEither = in.assetsIds
            .map { pbAssetId =>
              val readingContext = ContractReadingContext.TransactionExecution(claim.txId)

              val assetIdOpt = Option(pbAssetId).filter(_.nonEmpty)
              contractsApiService.contractAssetBalance(claim.contractId.base58, assetIdOpt, readingContext).map { contractAssetBalance =>
                ContractBalanceResponse(assetIdOpt, contractAssetBalance.amount, contractAssetBalance.decimals)
              }
            }
            .toList
            .sequence

          balancesEither
            .map(ContractBalancesResponse(_))
            .leftMap(_.asGrpcServiceException)
        }
    ).runToFuture(scheduler)
  }

  override def calculateAssetId(in: CalculateAssetIdRequest, metadata: Metadata): Future[AssetId] = {
    withRequestAuth(
      metadata,
      claim =>
        Task.eval {
          val txidBytes    = claim.txId.arr
          val nonceByte    = (in.nonce & 0xff).toByte
          val assetIdBytes = crypto.fastHash(txidBytes :+ nonceByte)
          AssetId(Base58.encode(assetIdBytes))
        }
    ).runToFuture(scheduler)
  }
}
