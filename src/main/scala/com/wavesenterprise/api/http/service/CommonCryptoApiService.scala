package com.wavesenterprise.api.http.service

import cats.implicits._
import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.JsonCryptoAlgo.possibleValuesMessage
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.{EncryptedForMany, EncryptedForSingle}
import com.wavesenterprise.protobuf.constants.CryptoAlgo
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.utils.{Base58, ScorexLogging}
import com.wavesenterprise.wallet.Wallet

abstract class CommonCryptoApiService(wallet: Wallet) extends ScorexLogging {

  /**
    * For every recipient from EncryptDataRequest we perform a separate encryption
    * As a result, every recipient gets his encryptedText and his wrappedKey to decrypt it
    */
  protected def encryptSeparate[T](sender: String,
                                   password: Option[String],
                                   dataToEncrypt: Array[Byte],
                                   recipientsPublicKeys: List[String],
                                   mapper: (String, EncryptedForSingle) => T,
                                   cryptoAlgo: CryptoAlgo): Either[ApiError, List[T]] = validateCryptoAlgo(cryptoAlgo) >> {
    val senderPrivateKey = for {
      senderAddress <- Address.fromString(sender).leftMap(ValidationError.fromCryptoError)
      senderPrivKey <- wallet.privateKeyAccount(senderAddress, password.map(_.toCharArray))
      _ = log.trace(s"successfully found private key for publicKey: '${senderPrivKey.publicKeyBase58}'")
    } yield senderPrivKey

    recipientsPublicKeys
      .traverse { recPubKeyStr =>
        /*_*/
        for {
          senderPrivKey   <- senderPrivateKey
          recipientPubKey <- PublicKeyAccount.fromBase58String(recPubKeyStr).leftMap(ValidationError.fromCryptoError)
          encrypted <- crypto
            .encrypt(dataToEncrypt, senderPrivKey.privateKey, recipientPubKey.publicKey)
            .leftMap { err =>
              GenericError(s"EncryptionSeparate failed: ${err.message}")
            }
          response = mapper(recPubKeyStr, encrypted)
        } yield response
      }
      /*_*/
      .leftMap(ApiError.fromValidationError)
  }

  /**
    * Data is encrypted only once on a common encryption key, and every recipient gets his own wrappedKey
    */
  protected def encryptCommon[T](sender: String,
                                 password: Option[String],
                                 dataToEncrypt: Array[Byte],
                                 recipientsPublicKeys: List[String],
                                 mapper: (Map[String, String], EncryptedForMany) => T,
                                 cryptoAlgo: CryptoAlgo): Either[ApiError, T] = validateCryptoAlgo(cryptoAlgo) >> {
    val maybeSenderPrivateKey: Either[ValidationError, PrivateKeyAccount] = for {
      senderAddress <- Address.fromString(sender).leftMap(ValidationError.fromCryptoError)
      senderPrivKey <- wallet.privateKeyAccount(senderAddress, password.map(_.toCharArray))
      _ = log.trace(s"successfully found private key for publicKey: '${senderPrivKey.publicKeyBase58}'")
    } yield senderPrivKey

    (for {
      senderPrivateKey        <- maybeSenderPrivateKey
      recipientPublicAccounts <- recipientsPublicKeys.traverse(PublicKeyAccount.fromBase58String(_).leftMap(ValidationError.fromCryptoError))
      recipientPubKeys = recipientPublicAccounts.map(_.publicKey)
      encryptResult <- crypto
        .encryptForMany(dataToEncrypt, senderPrivateKey.privateKey, recipientPubKeys)
        .leftMap(error => GenericError(error.message))
    } yield {
      val recipientToWrapped = encryptResult.recipientPubKeyToWrappedKey.map {
        case (pubKey, wrappedKey) =>
          val pubKeyBase58     = Base58.encode(pubKey.getEncoded)
          val wrappedKeyBase58 = Base58.encode(wrappedKey)
          pubKeyBase58 -> wrappedKeyBase58
      }
      mapper(recipientToWrapped, encryptResult)
    }).leftMap(ApiError.fromValidationError)
  }

  protected def decrypt[T](recipient: String,
                           password: Option[String],
                           encryptedData: Array[Byte],
                           wrappedKey: String,
                           senderPublicKey: String,
                           mapper: Array[Byte] => T,
                           cryptoAlgo: CryptoAlgo): Either[ApiError, T] = validateCryptoAlgo(cryptoAlgo) >> {
    (for {
      senderPublicKey <- PublicKeyAccount.fromBase58String(senderPublicKey).leftMap(ValidationError.fromCryptoError)
      _ = log.trace(s"senderPublicKey fromStr success: '${senderPublicKey.publicKeyBase58}'")
      recipientAddress <- Address.fromString(recipient).leftMap(ValidationError.fromCryptoError)
      _ = log.trace(s"recipientAddress fromStr success: '${recipientAddress.stringRepr}'")
      recipientPrivKey <- wallet.privateKeyAccount(recipientAddress, password.map(_.toCharArray))
      _ = log.trace(s"successfully get private key for recipientAddress, publicKeyBase58: '${recipientPrivKey.publicKeyBase58}'")
      decodedWrappedKey <- Base58.decode(wrappedKey).toEither.leftMap(ex => GenericError(ex.getLocalizedMessage))
      encryptedDataWithWrappedKey = EncryptedForSingle.apply(encryptedData, decodedWrappedKey)

      decryptedBytes <- crypto
        .decrypt(encryptedDataWithWrappedKey, recipientPrivKey.privateKey, senderPublicKey.publicKey)
        .leftMap { err =>
          GenericError(s"Decryption failed: ${err.message}")
        }

    } yield mapper(decryptedBytes)).leftMap(ApiError.fromValidationError)
  }

  private def validateCryptoAlgo(cryptoAlgo: CryptoAlgo): Either[ApiError, Unit] =
    cryptoAlgo match {
      case CryptoAlgo.AES => Right(())
      case CryptoAlgo.UNKNOWN_CRYPTO_ALGO =>
        Left(ApiError.CustomValidationError(s"Crypto algorithm is required"))
      case CryptoAlgo.Unrecognized(unknownAlgo) =>
        Left(ApiError.CustomValidationError(s"Unknown crypto algorithm '$unknownAlgo'. Possible values: $possibleValuesMessage"))
    }
}
