package com.wavesenterprise.api.http.service

import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8

import cats.data.EitherT
import cats.implicits._
import com.wavesenterprise.api.http.{ApiError, DecryptDataRequest, DecryptDataResponse, EncryptDataRequest}
import com.wavesenterprise.crypto.internals.EncryptedForMany
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.utils.{Base58, Base64}
import com.wavesenterprise.wallet.Wallet
import monix.eval.Task
import monix.execution.Scheduler
import play.api.libs.json.{Json, OFormat}

import scala.concurrent.Future

class CryptoApiService(wallet: Wallet, cryptoScheduler: Scheduler) extends CommonCryptoApiService(wallet) {

  /**
    * For every recipient from EncryptDataRequest we perform a separate encryption
    * As a result, every recipient gets his encryptedText and his wrappedKey to decrypt it
    */
  def encryptSeparate(request: EncryptDataRequest): Future[Either[ApiError, List[EncryptedSingleResponse]]] = {
    log.trace(
      s"received request to encryptSeparate. sender: '${request.sender}', recipients: [${request.recipientsPublicKeys.mkString(", ")}], text: '${request.encryptionText}'")
    EitherT
      .apply {
        Task.eval {
          encryptSeparate(
            request.sender,
            request.password,
            request.encryptionText.getBytes(UTF_8),
            request.recipientsPublicKeys,
            (recPubKeyStr, encrypted) => {
              EncryptedSingleResponse(
                encryptedText = Base64.encode(encrypted.encryptedData),
                publicKey = recPubKeyStr,
                wrappedKey = Base58.encode(encrypted.wrappedStructure)
              )
            },
            request.cryptoAlgo
          )
        }
      }
      .value
      .runToFuture(cryptoScheduler)
  }

  /**
    * Data is encrypted only once on a common encryption key, and every recipient gets his own wrappedKey
    */
  def encryptCommon(request: EncryptDataRequest): Future[Either[ApiError, EncryptedForManyResponse]] = {
    log.trace(
      s"received request to encryptCommon. sender: '${request.sender}', recipients: [${request.recipientsPublicKeys.mkString(", ")}], text: '${request.encryptionText}'")
    EitherT
      .apply {
        Task.eval {
          encryptCommon(
            request.sender,
            request.password,
            request.encryptionText.getBytes(UTF_8),
            request.recipientsPublicKeys,
            (recipientToWrapped, encrypted) => {
              val EncryptedForMany(encryptedData, _) = encrypted
              val encryptedDataBase64                = Base64.encode(encryptedData)

              EncryptedForManyResponse(
                encryptedText = encryptedDataBase64,
                recipientToWrappedStructure = recipientToWrapped
              )
            },
            request.cryptoAlgo
          )
        }
      }
      .value
      .runToFuture(cryptoScheduler)
  }

  def decrypt(request: DecryptDataRequest): Future[Either[ApiError, DecryptDataResponse]] = {
    log.trace(
      s"received request to decrypt. senderPublicKey: '${request.senderPublicKey}', recipient: '${request.recipient}', wrappedKey: '${request.wrappedKey}'")
    EitherT
      .apply {
        Task.eval {
          Base64.decode(request.encryptedText).toEither.leftMap(ex => ApiError.fromValidationError(GenericError(ex.getLocalizedMessage))).flatMap {
            encryptedData =>
              decrypt(
                request.recipient,
                request.password,
                encryptedData,
                request.wrappedKey,
                request.senderPublicKey,
                decryptedBytes => DecryptDataResponse(new String(decryptedBytes, StandardCharsets.UTF_8)),
                request.cryptoAlgo
              )
          }
        }
      }
      .value
      .runToFuture(cryptoScheduler)
  }
}

case class EncryptedSingleResponse(
    encryptedText: String,
    publicKey: String,
    wrappedKey: String
)

object EncryptedSingleResponse {
  implicit val format: OFormat[EncryptedSingleResponse] = Json.format
}

case class EncryptedForManyResponse(encryptedText: String, recipientToWrappedStructure: Map[String, String])

object EncryptedForManyResponse {
  implicit val format: OFormat[EncryptedForManyResponse] = Json.format
}
