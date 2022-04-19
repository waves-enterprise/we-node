package com.wavesenterprise.docker.grpc.service

import akka.grpc.scaladsl.Metadata
import cats.implicits._
import com.google.protobuf.ByteString
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.service.CommonCryptoApiService
import com.wavesenterprise.crypto.internals.EncryptedForMany
import com.wavesenterprise.docker._
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.wallet.Wallet
import monix.execution.Scheduler

import scala.concurrent.Future

class CryptoServiceImpl(val wallet: Wallet, val contractAuthTokenService: ContractAuthTokenService, val cryptoScheduler: Scheduler)
    extends CommonCryptoApiService(wallet)
    with CryptoServicePowerApi
    with WithServiceAuth {

  /**
    * For every recipient from EncryptDataRequest we perform a separate encryption
    * As a result, every recipient gets his encryptedText and his wrappedKey to decrypt it
    */
  override def encryptSeparate(request: EncryptDataRequest, metadata: Metadata): Future[EncryptSeparateResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        encryptSeparate(
          request.sender,
          request.password,
          request.encryptionData.toByteArray,
          request.recipientsPublicKeys.toList,
          (recPubKeyStr, encrypted) => {
            EncryptedSingleResponse(
              encryptedData = ByteString.copyFrom(encrypted.encryptedData),
              publicKey = recPubKeyStr,
              wrappedKey = Base58.encode(encrypted.wrappedStructure)
            )
          },
          request.cryptoAlgo
        ).bimap(_.asGrpcServiceException, EncryptSeparateResponse(_))
      }
    ).runToFuture(cryptoScheduler)

  /**
    * Data is encrypted only once on a common encryption key, and every recipient gets his own wrappedKey
    */
  override def encryptCommon(request: EncryptDataRequest, metadata: Metadata): Future[EncryptedForManyResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        encryptCommon(
          request.sender,
          request.password,
          request.encryptionData.toByteArray,
          request.recipientsPublicKeys.toList,
          (recipientToWrapped, encrypted) => {
            val EncryptedForMany(encryptedData, _) = encrypted
            EncryptedForManyResponse(encryptedData = ByteString.copyFrom(encryptedData), recipientToWrappedStructure = recipientToWrapped)
          },
          request.cryptoAlgo
        ).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(cryptoScheduler)

  override def decrypt(request: DecryptDataRequest, metadata: Metadata): Future[DecryptDataResponse] =
    withRequestAuth(
      metadata,
      deferEither {
        decrypt(
          request.recipient,
          request.password,
          request.encryptedData.toByteArray,
          request.wrappedKey,
          request.senderPublicKey,
          decryptedBytes => DecryptDataResponse(ByteString.copyFrom(decryptedBytes)),
          request.cryptoAlgo
        ).leftMap(_.asGrpcServiceException)
      }
    ).runToFuture(cryptoScheduler)
}
