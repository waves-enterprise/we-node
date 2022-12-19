package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.wavesenterprise.api.http.ApiError.{BlockDoesNotExist, CustomValidationError}
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.docker.{ContractAuthTokenService, deferEither}
import com.wavesenterprise.protobuf.service.contract.{BlockHeader, BlockHeaderRequest, BlockServicePowerApi}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import monix.execution.Scheduler

import scala.concurrent.Future

class BlockServiceImpl(
    val blockchain: Blockchain,
    val contractAuthTokenService: ContractAuthTokenService,
    val scheduler: Scheduler
) extends BlockServicePowerApi
    with WithServiceAuth {
  override def getBlockHeader(request: BlockHeaderRequest, metadata: Metadata): Future[BlockHeader] =
    withRequestAuth(
      metadata,
      deferEither {
        def findByHeight =
          request.blockRef.height
            .map(height =>
              blockchain
                .blockHeaderAndSize(height.toInt)
                .map { case (blockHeader, _) => (blockHeader, height) }
                .toRight(BlockDoesNotExist(height.toInt.asRight).asGrpcServiceException))

        def findByBlockId =
          for {
            inputSignature <- request.blockRef.signature
            signature      <- ByteStr.decodeBase58(inputSignature).toOption
            height         <- blockchain.heightOf(signature)
          } yield blockchain
            .blockHeaderAndSize(signature)
            .map { case (blockHeader, _) => (blockHeader, height.toLong) }
            .toRight(BlockDoesNotExist(signature.asLeft).asGrpcServiceException)
        findByHeight
          .orElse(findByBlockId)
          .getOrElse(CustomValidationError("Empty both fields 'signature' and 'height' in request").asGrpcServiceException.asLeft)
          .map {
            case (blockHeader, height) =>
              BlockHeader(
                version = blockHeader.version,
                height = height,
                blockSignature = blockHeader.signerData.signature.base58,
                reference = blockHeader.reference.base58,
                minerAddress = blockHeader.sender.address,
                txCount = blockHeader.transactionCount,
                timestamp = blockHeader.timestamp
              )
          }
      }
    ).runToFuture(scheduler)
}
