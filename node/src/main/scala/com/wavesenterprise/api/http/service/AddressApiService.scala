package com.wavesenterprise.api.http.service

import cats.syntax.either._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.http.AddressApiRoute.{
  AddressPublicKeyInfo,
  AddressScriptInfo,
  Balance,
  BalanceDetails,
  GeneratingBalance,
  Signed,
  VerificationResult
}
import com.wavesenterprise.api.http.ApiError.{DataKeyNotExists, InvalidMessage, InvalidPublicKey, InvalidSignature}
import com.wavesenterprise.api.http.utils.AccountFromAddressOrAliasString
import com.wavesenterprise.api.http.{ApiError, Message, SignedMessage}
import com.wavesenterprise.consensus.GeneratingBalanceProvider
import com.wavesenterprise.crypto
import com.wavesenterprise.settings.FunctionalitySettings
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.smart.script.ScriptCompiler
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.wallet.Wallet

import java.nio.charset.StandardCharsets

class AddressApiService(val blockchain: Blockchain, wallet: Wallet, functionalitySettings: FunctionalitySettings)
    extends AccountFromAddressOrAliasString {

  def accountData(addressOrAlias: String, offset: Option[Int], limit: Option[Int]): Either[ApiError, Seq[DataEntry[_]]] = {
    accountFrom(addressOrAlias)
      .leftMap(ApiError.fromValidationError)
      .flatMap(getAccountData(_, offset, limit))
  }

  private def balance(address: Address): Balance = Balance(address.address, 0, blockchain.addressBalance(address))

  def balance(addressOrAlias: String): Either[ValidationError, Balance] = {
    accountFrom(addressOrAlias).map(balance)
  }

  def addressScriptInfo(addressOrAlias: String): Either[ValidationError, AddressScriptInfo] = {
    accountFrom(addressOrAlias).flatMap(addressScriptInfoJson)
  }

  def addressScriptInfoJson(account: Address): Either[ValidationError, AddressScriptInfo] =
    for {
      script <- Right(blockchain.accountScript(account))
      complexity <- script.fold[Either[ValidationError, Long]](Right(0))(script =>
        ScriptCompiler.estimate(script, script.version).left.map(GenericError(_)))
    } yield AddressScriptInfo(
      address = account.address,
      script = script.map(_.bytes().base64),
      scriptText = script.map(_.text),
      complexity = complexity
    )

  def balancesDetailsFromStr(addressOrAlias: String): Either[ValidationError, BalanceDetails] = {
    accountFrom(addressOrAlias).map(balancesDetails)
  }

  def balancesDetails(account: Address): BalanceDetails = {
    val portfolio = blockchain.addressWestPortfolio(account)
    BalanceDetails(
      address = account.address,
      regular = portfolio.balance,
      generating = GeneratingBalanceProvider.balance(blockchain, functionalitySettings, blockchain.height, account),
      available = portfolio.spendableBalance,
      effective = portfolio.effectiveBalance
    )
  }

  def balancesDetailsOrError(addressOrAlias: String): Either[ApiError, BalanceDetails] = {
    accountFrom(addressOrAlias)
      .map(balancesDetails)
      .leftMap(ApiError.fromValidationError)
  }

  def effectiveBalance(addressOrAlias: String, confirmations: Int): Either[ValidationError, Balance] = {
    accountFrom(addressOrAlias)
      .map(acc => Balance(acc.address, confirmations, blockchain.effectiveBalance(acc, blockchain.height, confirmations)))
  }

  def accountData(addressOrAlias: String, key: String): Either[ApiError, DataEntry[_]] = {
    for {
      acc   <- accountFrom(addressOrAlias).leftMap(ApiError.fromValidationError)
      value <- blockchain.accountData(acc, key).toRight(DataKeyNotExists(key))
    } yield value
  }

  def calculateGenBalance(addressOrAlias: String, requestedHeight: Int): Either[ApiError, GeneratingBalance] = {
    accountFrom(addressOrAlias).leftMap(ApiError.fromValidationError).map { account =>
      val calculatedGenBalance = GeneratingBalanceProvider.balance(blockchain, requestedHeight, account)
      GeneratingBalance(addressOrAlias, calculatedGenBalance)
    }
  }

  def verifySignedMessage(m: SignedMessage, address: String, isMessageEncoded: Boolean): Either[ApiError, VerificationResult] = {
    def decodeOrInvalidMessage(input: String, error: ApiError, nonEmpty: Boolean): Either[ApiError, Array[Byte]] = {
      if (nonEmpty && input.isEmpty) Left(error)
      else Base58.decode(input).toEither.leftMap(_ => error)
    }

    for {
      signerAddress <- Address.fromString(address).leftMap(ApiError.fromCryptoError)
      msg <- if (isMessageEncoded)
        decodeOrInvalidMessage(m.message, InvalidMessage, nonEmpty = false)
      else
        Right(m.message.getBytes(StandardCharsets.UTF_8))
      signature        <- decodeOrInvalidMessage(m.signature, InvalidSignature, nonEmpty = true)
      publicKeyBytes   <- decodeOrInvalidMessage(m.publickey, InvalidPublicKey(m.publickey), nonEmpty = true)
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
