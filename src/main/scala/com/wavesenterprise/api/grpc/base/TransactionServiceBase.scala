package com.wavesenterprise.api.grpc.base

import akka.grpc.GrpcServiceException
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.ApiError.UnimplementedError
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.{ProtoSerializableTransaction, ValidationError}
import com.wavesenterprise.protobuf.service.util.TransactionInfoResponse

trait TransactionServiceBase {

  def blockchain: Blockchain

  protected def transactionInfo(txId: String): Either[GrpcServiceException, TransactionInfoResponse] = {
    parseTxId(txId).flatMap { txId =>
      blockchain.transactionInfo(txId) match {
        case Some((height, tx: ProtoSerializableTransaction)) =>
          Right(TransactionInfoResponse(height, Some(tx.toProto)))
        case Some(_) =>
          Left(UnimplementedError("Protobuf serialization not available for this type of transaction").asGrpcServiceException)
        case None =>
          Left(ValidationError.TransactionNotFound(txId).asGrpcServiceException)
      }
    }
  }
}
