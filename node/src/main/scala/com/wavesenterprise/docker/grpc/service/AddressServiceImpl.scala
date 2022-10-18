package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.service.AddressApiService
import com.wavesenterprise.crypto
import com.wavesenterprise.docker.grpc.ProtoObjectsMapper
import com.wavesenterprise.docker.{deferEither, _}
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.serialization.ProtoAdapter
import com.wavesenterprise.settings.Constants
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.ValidationError.InvalidAssetId
import com.wavesenterprise.transaction.{AssetId, ValidationError}
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class AddressServiceImpl(val addressApiService: AddressApiService, val contractAuthTokenService: ContractAuthTokenService, val scheduler: Scheduler)
    extends AddressServicePowerApi
    with WithServiceAuth {

  override def getAddresses(request: Empty, metadata: Metadata): Future[AddressesResponse] =
    withRequestAuth(metadata, Task {
      AddressesResponse(addressApiService.addressesFromWallet())
    }).runToFuture(scheduler)

  override def getAddressData(request: AddressDataRequest, metadata: Metadata): Future[AddressDataResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        (for {
          values <- addressApiService.accountData(request.address, request.offset, request.limit)
          protoValues = values.map(ProtoObjectsMapper.mapToProto)
        } yield AddressDataResponse(protoValues)).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(scheduler)

  override def getAssetBalance(in: AssetBalanceRequest, metadata: Metadata): Future[AssetBalanceResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        (for {
          address       <- ProtoAdapter.addressFromProto(in.address)
          maybeAssetId  <- extractAssetId(in.assetId)
          assetDecimals <- findAssetDecimals(addressApiService.blockchain, maybeAssetId)
          amount = addressApiService.blockchain.addressBalance(address, maybeAssetId)
        } yield AssetBalanceResponse(in.assetId, amount, assetDecimals)).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(scheduler)

  private def findAssetDecimals(blockchain: Blockchain, maybeAssetId: Option[AssetId]): Either[ValidationError, Int] = {
    maybeAssetId.fold {
      Either.right[ValidationError, Int](Constants.WestDecimals)
    } { assetId =>
      blockchain
        .assetDescription(assetId)
        .map(_.decimals.toInt)
        .toRight(InvalidAssetId(s"Unable to find a description for AssetId '${assetId.base58}'"))
    }
  }

  private def extractAssetId(maybeProtoAssetId: Option[ByteString]): Either[ValidationError, Option[AssetId]] = {
    maybeProtoAssetId.fold {
      Either.right[ValidationError, Option[ByteStr]](None)
    } { protoAssetId =>
      Either.cond[ValidationError, Unit](
        protoAssetId.size() == crypto.DigestSize,
        (),
        InvalidAssetId(s"Invalid AssetId length '${protoAssetId.size()}', expected: '${crypto.DigestSize}'")
      ) >> ProtoAdapter.byteStrFromProto(protoAssetId).map(Some(_))
    }
  }
}
