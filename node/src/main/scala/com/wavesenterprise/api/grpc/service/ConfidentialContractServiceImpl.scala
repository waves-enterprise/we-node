package com.wavesenterprise.api.grpc.service

import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import cats.data.EitherT
import cats.implicits._
import com.google.protobuf.ByteString
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.grpc.utils.{ApiErrorExt, ValidationErrorExt, parseAtomicBadge}
import com.wavesenterprise.api.http.service.confidentialcontract._
import com.wavesenterprise.docker.grpc.ProtoObjectsMapper
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.serialization.ProtoAdapter
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.protobuf.{
  ContractKeysRequest,
  ContractKeysResponse,
  KeysFilter,
  ConfidentialInput => PbConfidentialInput,
  ConfidentialOutput => PbConfidentialOutput
}
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class ConfidentialContractServiceImpl(
    override val authSettings: AuthorizationSettings,
    override val nodeOwner: Address,
    override val time: Time,
    confidentialContractsApiService: ConfidentialContractsApiService
)(implicit val s: Scheduler) extends ConfidentialContractServicePowerApi with GrpcAuth {
  implicit def byteArrayToByteString(byteStr: Array[Byte]): ByteString = ByteString.copyFrom(byteStr)
  implicit def byteStrToPbByteString(byteStr: ByteStr): ByteString     = ByteString.copyFrom(byteStr.arr)

  override def confidentialCall(in: ConfidentialCallRequest, metadata: Metadata): Future[ConfidentialCallResponse] = {
    withAuthTask(metadata) {
      (for {
        maybeAtomicBadge <- parseAtomicBadge(in.atomicBadge).leftMap(_.asGrpcServiceException).toEitherT[Task]
        params           <- in.params.toList.traverse(data => ProtoAdapter.fromProto(data)).leftMap(_.asGrpcServiceException).toEitherT[Task]

        request = ConfidentialContractCallRequest(
          sender = in.sender,
          password = in.password,
          contractId = in.contractId,
          contractVersion = in.contractVersion,
          params = params,
          timestamp = in.timestamp,
          atomicBadge = maybeAtomicBadge,
          fee = in.fee,
          feeAssetId = in.feeAssetId,
          commitment = in.commitment,
          commitmentKey = in.commitmentKey,
          certificatesBytes = in.certificates.map(_.toByteArray).toList
        )

        response <- EitherT(confidentialContractsApiService.call(
          request,
          in.broadcastTx,
          in.commitmentVerification
        ).map(_.leftMap(_.asGrpcServiceException)))

        pbConfidentialInput = PbConfidentialInput(
          commitmentHash = response.confidentialInput.commitment.hash,
          txId = response.confidentialInput.txId.base58,
          contractId = response.confidentialInput.contractId.byteStr.base58,
          commitmentKey = response.confidentialInput.commitmentKey.bytes,
          entries = response.confidentialInput.entries.map(ProtoAdapter.toProto),
        )

        pbResponse = ConfidentialCallResponse(
          transaction = response.callContractTransactionV6.toInnerProto.some,
          confidentialInput = pbConfidentialInput.some
        )
      } yield pbResponse).value.flatMap {
        case Right(value) => Task(value)
        case Left(err)    => Task.raiseError(err)
      }
    }
  }.runToFuture

  override def confidentialExecutedTxByExecutableTxId(in: ExecutedTxRequest, metadata: Metadata): Future[ExecutedTxByIdResponse] = {
    withAuthTask(metadata) {
      (for {
        response <- confidentialContractsApiService.confidentialTxByExecutableTxId(in.transactionId).leftMap(_.asGrpcServiceException)

        pbConfidentialInput = PbConfidentialInput(
          commitmentHash = response.confidentialInput.commitment.hash,
          txId = response.confidentialInput.txId.base58,
          contractId = response.confidentialInput.contractId.byteStr.base58,
          commitmentKey = response.confidentialInput.commitmentKey.bytes,
          entries = response.confidentialInput.entries.map(ProtoAdapter.toProto),
        )

        pbConfidentialOutput = PbConfidentialOutput(
          commitmentHash = response.confidentialOutput.commitment.hash,
          txId = response.confidentialOutput.txId.base58,
          contractId = response.confidentialOutput.contractId.byteStr.base58,
          commitmentKey = response.confidentialOutput.commitmentKey.bytes,
          entries = response.confidentialOutput.entries.map(ProtoAdapter.toProto),
        )

        pbResponse = ExecutedTxByIdResponse(
          transaction = response.executedContractTransactionV4.toInnerProto.some,
          confidentialInput = pbConfidentialInput.some,
          confidentialOutput = pbConfidentialOutput.some
        )
      } yield pbResponse) match {
        case Right(value) => Task(value)
        case Left(err)    => Task.raiseError(err)
      }
    }
  }.runToFuture

  override def getContractKeys(in: ContractKeysRequest, metadata: Metadata): Future[ContractKeysResponse] = {
    withAuthTask(metadata) {
      (for {
        response <- getContractKeysInner(in)
        protoValues = response.map(ProtoObjectsMapper.mapToProto)
      } yield ContractKeysResponse(protoValues)) match {
        case Right(value) => Task(value)
        case Left(err)    => Task.raiseError(err)
      }
    }
  }.runToFuture

  private def getContractKeysInner(request: ContractKeysRequest): Either[GrpcServiceException, Vector[DataEntry[_]]] = {
    (request.matches match {
      case Some(regex) => confidentialContractsApiService.contractKeys(request.contractId, request.offset, request.limit, Some(regex))
      case None =>
        request.keysFilter match {
          case Some(KeysFilter(keys, _)) => confidentialContractsApiService.contractKeys(request.contractId, keys)
          case None                      => confidentialContractsApiService.contractKeys(request.contractId, request.offset, request.limit, None)
        }
    }).leftMap(_.asGrpcServiceException)
  }

}
