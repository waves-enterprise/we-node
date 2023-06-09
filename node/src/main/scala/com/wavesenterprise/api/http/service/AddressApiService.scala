package com.wavesenterprise.api.http.service

import cats.syntax.either._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.http.AddressApiRoute.{AddressPublicKeyInfo, Signed, VerificationResult}
import com.wavesenterprise.api.http.ApiError.{InvalidMessage, InvalidPublicKey, InvalidSignature}
import com.wavesenterprise.api.http.{ApiError, Message, SignedMessage}
import com.wavesenterprise.crypto
import com.wavesenterprise.state.{Blockchain, DataEntry, _}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.wallet.Wallet

import java.nio.charset.StandardCharsets

class AddressApiService(val blockchain: Blockchain, wallet: Wallet) {

  def accountData(address: String, offset: Option[Int], limit: Option[Int]): Either[ApiError, Seq[DataEntry[_]]] = {
    Address
      .fromString(address)
      .leftMap(ApiError.fromCryptoError)
      .flatMap(getAccountData(_, offset, limit))
  }

  def verifySignedMessage(m: SignedMessage, address: String, isMessageEncoded: Boolean): Either[ApiError, VerificationResult] = {
    def decodeOrInvalidMessage(input: String, error: ApiError): Either[ApiError, Array[Byte]] =
      Base58.decode(input).toEither.leftMap(_ => error)

    for {
      signerAddress <- Address.fromString(address).leftMap(ApiError.fromCryptoError)
      msg <- if (isMessageEncoded)
        decodeOrInvalidMessage(m.message, InvalidMessage)
      else
        Right(m.message.getBytes(StandardCharsets.UTF_8))
      signature        <- decodeOrInvalidMessage(m.signature, InvalidSignature)
      publicKeyBytes   <- decodeOrInvalidMessage(m.publickey, InvalidPublicKey(m.publickey))
      publicKeyAccount <- PublicKeyAccount.fromBytes(publicKeyBytes).leftMap(ApiError.fromCryptoError)
    } yield {
      val isValid = publicKeyAccount.toAddress == signerAddress && crypto.verify(signature, msg, publicKeyAccount.publicKey)
      VerificationResult(isValid)
    }
  }

  def signMessage(request: Message, address: String, encodeMessage: Boolean): Either[ValidationError, Signed] = {
    wallet
      .findPrivateKey(address, request.password.map(_.toCharArray))
      .map { pk =>
        val messageBytes = request.message.getBytes(StandardCharsets.UTF_8)
        val signature    = crypto.sign(pk, messageBytes)
        val msg          = if (encodeMessage) Base58.encode(messageBytes) else request.message
        Signed(msg, pk.publicKeyBase58, Base58.encode(signature))
      }
  }

  def addressInfoFromWallet(addressStr: String): Either[ValidationError, AddressPublicKeyInfo] = {
    for {
      parsedAddress <- Address.fromString(addressStr).leftMap(ValidationError.fromCryptoError)
      publicAccount <- wallet.publicKeyAccount(parsedAddress)
    } yield AddressPublicKeyInfo(parsedAddress.address, publicAccount.publicKeyBase58)
  }

  def addressesFromWallet(): List[String] = {
    wallet.publicKeyAccounts.map(_.address)
  }

  private def getAccountData(address: Address, offsetOpt: Option[Int], limitOpt: Option[Int]): Either[ApiError, Seq[DataEntry[_]]] = {
    (offsetOpt, limitOpt) match {
      case (None, None)         => Right(getDataEntry(address, 0, Integer.MAX_VALUE))
      case (Some(offset), None) => Right(getDataEntry(address, offset.positiveOrZero, Integer.MAX_VALUE))
      case (None, Some(limit))  => Right(getDataEntry(address, 0, limit))
      case (Some(offset), Some(limit)) =>
        val realOffset = offset.positiveOrZero
        validateSum(realOffset, limit).map(to => getDataEntry(address, realOffset, to))
    }
  }

  private def getDataEntry(acc: Address, from: Int, to: Int): Seq[DataEntry[_]] = {
    blockchain.accountDataSlice(acc, from, to).data.values.toSeq.sortBy(_.key)
  }

}
