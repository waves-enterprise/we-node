package com.wavesenterprise.api.http

import akka.http.scaladsl.server.Route
import cats.implicits.{catsStdBitraverseForEither, catsStdInstancesForList, catsSyntaxAlternativeSeparate}
import cats.syntax.either.catsSyntaxEither
import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.api.ValidInt._
import com.wavesenterprise.api.http.ApiError.{CustomValidationError, DataKeyNotExists, RequestedHeightDoesntExist, TooBigArrayAllocation}
import com.wavesenterprise.api.http.auth.WithAuthFromContract
import com.wavesenterprise.api.http.service.AddressApiService
import com.wavesenterprise.consensus.GeneratingBalanceProvider
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.settings.{ApiSettings, FunctionalitySettings}
import com.wavesenterprise.state.{Blockchain, DataEntry}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.smart.script.ScriptCompiler
import com.wavesenterprise.utils.{Base58, Time}
import com.wavesenterprise.utx.UtxPool
import monix.execution.schedulers.SchedulerService
import play.api.libs.json._

class AddressApiRoute(addressApiService: AddressApiService,
                      val settings: ApiSettings,
                      val time: Time,
                      blockchain: Blockchain,
                      val utx: UtxPool,
                      val functionalitySettings: FunctionalitySettings,
                      val contractAuthTokenService: Option[ContractAuthTokenService],
                      val nodeOwner: Address,
                      val scheduler: SchedulerService)
    extends ApiRoute
    with WithAuthFromContract
    with AdditionalDirectiveOps {

  import AddressApiRoute._

  val MaxAddressesPerRequest = 1000

  private val userAuth           = withAuth()
  private val contractOrUserAuth = withContractAuth | userAuth

  override lazy val route: Route =
    pathPrefix("addresses") {
      validate ~ validateMany ~ balanceDetails ~ addressesBalanceDetails ~ balance ~ verify ~ sign ~ verifyText ~
        signText ~ seq ~ publicKey ~ effectiveBalance ~ effectiveBalanceWithConfirmations ~ generatingBalanceAtHeight ~ getData ~ getDataItem ~ scriptInfo ~ info
    } ~ root

  /**
    * GET /addresses/info/{address}
    *
    * Return address's public key, if it is found in the wallet
    */
  def info: Route = (get & path("info" / Segment) & userAuth) { addressStr =>
    withExecutionContext(scheduler) {
      complete {
        addressApiService.addressInfoFromWallet(addressStr)
      }
    }
  }

  /**
    * GET /addresses/scriptInfo/{address}
    *
    * Details for account's script
    **/
  def scriptInfo: Route = (get & path("scriptInfo" / Segment) & userAuth) { address =>
    withExecutionContext(scheduler) {
      complete(
        Address
          .fromString(address)
          .leftMap(ValidationError.fromCryptoError)
          .flatMap(addressScriptInfoJson)
      )
    }
  }

  /**
    * POST /addresses/sign/{address}
    *
    * Sign a message with a private key associated with {address}
    **/
  def sign: Route = (post & path("sign" / Segment) & userAuth & addedGuard) { address =>
    withExecutionContext(scheduler) {
      json[Message] { request =>
        addressApiService.signMessage(request, address, encodeMessage = true)
      }
    }
  }

  /**
    * POST /addresses/signText/{address}
    *
    * Sign a message with a private key associated with {address}
    **/
  def signText: Route = (post & path("signText" / Segment) & userAuth & addedGuard) { address =>
    withExecutionContext(scheduler) {
      json[Message] { request =>
        addressApiService.signMessage(request, address, encodeMessage = false)
      }
    }
  }

  /**
    * POST /addresses/verify/{address}
    *
    * Check a signature of a message signed by an account
    **/
  def verify: Route = (post & path("verify" / Segment) & userAuth & addedGuard) { address =>
    withExecutionContext(scheduler) {
      json[SignedMessage] { signedMessage =>
        addressApiService.verifySignedMessage(signedMessage, address, isMessageEncoded = true)
      }
    }
  }

  /**
    * POST /addresses/verifyText/{address}
    *
    * Check a signature of a message signed by an account
    **/
  def verifyText: Route = (post & path("verifyText" / Segment) & userAuth & addedGuard) { address =>
    withExecutionContext(scheduler) {
      json[SignedMessage] { signedMessage =>
        addressApiService.verifySignedMessage(signedMessage, address, isMessageEncoded = false)
      }
    }
  }

  /**
    * GET /addresses/balance/{address}
    **/
  def balance: Route = (get & path("balance" / Segment) & contractOrUserAuth) { address =>
    withExecutionContext(scheduler) {
      complete(balanceJson(address))
    }
  }

  /**
    * GET /addresses/balance/details/{address}
    **/
  def balanceDetails: Route = (get & path("balance" / "details" / Segment) & userAuth) { addressStr =>
    withExecutionContext(scheduler) {
      complete(
        Address
          .fromString(addressStr)
          .map(balancesDetails)
          .leftMap(ApiError.fromCryptoError)
      )
    }
  }

  /**
    * POST /addresses/balance/details
    **/
  def addressesBalanceDetails: Route = (post & path("balance" / "details") & userAuth) {
    withExecutionContext(scheduler) {
      json[AddressesMessage] { message =>
        {
          if (message.addresses.length <= MaxAddressesPerRequest) {
            val (errorsJson, balancesDetailsJson) = message.addresses.toList
              .map(balancesDetailsOrErrorJson)
              .separate

            balancesDetailsJson ++ errorsJson
          } else {
            TooBigArrayAllocation
          }
        }
      }
    }
  }

  /**
    * GET /addresses/effectiveBalance/{address}
    **/
  def effectiveBalance: Route = (get & path("effectiveBalance" / Segment) & userAuth) { address =>
    withExecutionContext(scheduler) {
      complete(effectiveBalanceJson(address, 0))
    }
  }

  /**
    * GET /addresses/effectiveBalance/{address}/{confirmations}
    *
    * Balance of {address} after {confirmations}
    **/
  def effectiveBalanceWithConfirmations: Route = (get & path("effectiveBalance" / Segment / Segment) & userAuth) { (address, confirmationsStr) =>
    withExecutionContext(scheduler) {
      PositiveInt(confirmationsStr).processRoute { confirmations =>
        complete(effectiveBalanceJson(address, confirmations))
      }
    }
  }

  /**
    * GET /addresses/generatingBalance/{address}/at/{height}
    *
    * Generating balance of {address} at {height}
    */
  def generatingBalanceAtHeight: Route = (get & path("generatingBalance" / Segment / "at" / Segment) & userAuth) { (address, requestedHeightStr) =>
    withExecutionContext(scheduler) {
      PositiveInt(requestedHeightStr).processRoute { requestedHeight =>
        complete {
          val currentHeight = blockchain.height
          for {
            _             <- Either.cond(requestedHeight <= currentHeight, (), RequestedHeightDoesntExist(requestedHeight, currentHeight))
            parsedAddress <- Address.fromString(address).leftMap(ApiError.fromCryptoError)
            calculatedGenBalance = GeneratingBalanceProvider.balance(blockchain, requestedHeight, parsedAddress)
          } yield GeneratingBalance(address, calculatedGenBalance)
        }
      }
    }
  }

  /**
    * Checking validity of AddressOrAlias
    * If it's an alias, check presence in the state
    */
  def checkAddressOrAliasValid(inputStr: String): Either[ValidationError, Unit] = {
    Base58.decode(inputStr).toEither match {
      case Right(decoded) if decoded.length == Address.AddressLength =>
        Address.fromBytes(decoded).leftMap(ValidationError.fromCryptoError)

      case _ =>
        val alias =
          if (!inputStr.startsWith("alias"))
            Alias.buildWithCurrentChainId(inputStr)
          else
            Alias.fromString(inputStr)
        alias.leftMap(ValidationError.fromCryptoError).flatMap(blockchain.resolveAlias)
    }
  }.map(_ => ())

  /**
    * GET /addresses/validate/{addressOrAlias}
    *
    * Check whether address or alias {addressOrAlias} is valid or not
    **/
  def validate: Route = (get & path("validate" / Segment) & userAuth) { addressOrAliasStr =>
    withExecutionContext(scheduler) {
      complete {
        checkAddressOrAliasValid(addressOrAliasStr) match {
          case Right(_)  => ValiditySingle(addressOrAliasStr, valid = true, None)
          case Left(err) => ValiditySingle(addressOrAliasStr, valid = false, Some(err.toString))
        }
      }
    }
  }

  /**
    * POST /addresses/validateMany
    *
    * Check whether addresses or aliases is valid or not
    **/
  def validateMany: Route = (post & path("validateMany") & userAuth) {
    withExecutionContext(scheduler) {
      json[ValidateManyReq] { req =>
        val validations = req.addressesOrAliases.map { addressOrAliasStr =>
          checkAddressOrAliasValid(addressOrAliasStr) match {
            case Right(_)  => ValiditySingle(addressOrAliasStr, valid = true, None)
            case Left(err) => ValiditySingle(addressOrAliasStr, valid = false, Some(err.toString))
          }
        }
        ValidityMany(validations)
      }
    }
  }

  /**
    * GET /addresses/data/{address}
    *
    * Read all data posted by an account
    **/
  def getData: Route = (get & path("data" / Segment) & contractOrUserAuth) { address =>
    withExecutionContext(scheduler) {
      parameters('offset.as[String].?, 'limit.as[String].?) { (offsetStr, limitStr) =>
        List(offsetStr.map(NonNegativeInt(_)), limitStr.map(PositiveInt(_))).flatten.processRoute { _ =>
          complete(addressApiService.accountData(address, offsetStr.map(_.toInt), limitStr.map(_.toInt)))
        }
      }
    }
  }

  /**
    * GET /addresses/data/{address}/{key}
    *
    * Read data associated with an account and a key
    **/
  def getDataItem: Route = (get & path("data" / Segment / Segment.?) & contractOrUserAuth) {
    case (address, keyOpt) =>
      withExecutionContext(scheduler) {
        complete(accountData(address, keyOpt.getOrElse("")))
      }
  }

  /**
    * GET /addresses
    *
    * Get wallet accounts addresses
    **/
  def root: Route = (get & path("addresses") & contractOrUserAuth) {
    withExecutionContext(scheduler) {
      val accounts = addressApiService.addressesFromWallet()
      val json     = JsArray(accounts.map(JsString.apply))
      complete(json)
    }
  }

  /**
    * GET /addresses/seq/{from}/{to}
    *
    * Get wallet accounts addresses
    **/
  def seq: Route = (get & path("seq" / Segment / Segment) & userAuth) { (startStr, endStr) =>
    withExecutionContext(scheduler) {
      List(NonNegativeInt(startStr), PositiveInt(endStr)).processRoute {
        case Seq(start, end) =>
          if (end - start > MaxAddressesPerRequest) {
            complete(TooBigArrayAllocation)
          } else if (end < start) {
            complete(CustomValidationError("Invalid interval"))
          } else {
            val json = JsArray(
              addressApiService.addressesFromWallet().map(JsString.apply).slice(start, end + 1)
            )
            complete(json)
          }
      }
    }
  }

  /**
    * GET /addresses/publicKey/{publicKey}
    *
    * Generate a address from public key
    **/
  def publicKey: Route = (get & path("publicKey" / Segment) & contractOrUserAuth) { publicKeyStr =>
    withExecutionContext(scheduler) {
      complete {
        PublicKeyAccount
          .fromBase58String(publicKeyStr)
          .map { pubKey =>
            AddressResponse(pubKey.address)
          }
          .leftMap(ApiError.fromCryptoError)
      }
    }
  }

  private def balanceJson(address: String): Either[ValidationError, Balance] = {
    Address
      .fromString(address)
      .map { acc =>
        Balance(acc.address, 0, blockchain.addressBalance(acc))
      }
      .leftMap(ValidationError.fromCryptoError)
  }

  private def balancesDetails(account: Address): BalanceDetails = {
    val portfolio = blockchain.westPortfolio(account)
    BalanceDetails(
      address = account.address,
      regular = portfolio.balance,
      generating = GeneratingBalanceProvider.balance(blockchain, functionalitySettings, blockchain.height, account),
      available = portfolio.spendableBalance,
      effective = portfolio.effectiveBalance
    )
  }

  private def balancesDetailsOrErrorJson(address: String): Either[JsValue, JsValue] =
    Address
      .fromString(address)
      .bimap(
        error => Json.obj("address" -> address) ++ ApiError.fromCryptoError(error).json,
        address => Json.toJson(balancesDetails(address))
      )

  private def addressScriptInfoJson(account: Address): Either[ValidationError, AddressScriptInfo] =
    for {
      script <- Right(blockchain.accountScript(account))
      complexity <- script.fold[Either[ValidationError, Long]](Right(0))(script =>
        ScriptCompiler.estimate(script, script.version).left.map(GenericError(_)))
    } yield
      AddressScriptInfo(
        address = account.address,
        script = script.map(_.bytes().base64),
        scriptText = script.map(_.text),
        complexity = complexity
      )

  private def effectiveBalanceJson(address: String, confirmations: Int): Either[ValidationError, Balance] = {
    Address
      .fromString(address)
      .map(acc => Balance(acc.address, confirmations, blockchain.effectiveBalance(acc, blockchain.height, confirmations)))
      .leftMap(ValidationError.fromCryptoError)
  }

  private def accountData(address: String, key: String): Either[ApiError, DataEntry[_]] = {
    for {
      addr  <- Address.fromString(address).leftMap(ApiError.fromCryptoError)
      value <- blockchain.accountData(addr, key).toRight(DataKeyNotExists)
    } yield value
  }
}

object AddressApiRoute {

  case class AddressResponse(address: String)

  implicit val addressFormat: Format[AddressResponse] = Json.format

  case class Signed(message: String, publicKey: String, signature: String)

  implicit val signedFormat: Format[Signed] = Json.format

  case class VerificationResult(valid: Boolean)

  implicit val verificationResultFormat: Format[VerificationResult] = Json.format

  case class AddressSimplified(address: String)

  implicit val AddressFormat: Format[AddressSimplified] = Json.format

  case class Balance(address: String, confirmations: Int, balance: Long)

  implicit val balanceFormat: Format[Balance] = Json.format

  case class BalanceDetails(address: String, regular: Long, generating: Long, available: Long, effective: Long)

  implicit val balanceDetailsFormat: Format[BalanceDetails] = Json.format

  case class GeneratingBalance(address: String, balance: Long)

  implicit val generatingBalanceFormat: Format[GeneratingBalance] = Json.format

  case class ValidateManyReq(addressesOrAliases: Seq[String])

  implicit val validateManyReq: Format[ValidateManyReq] = Json.format

  case class ValiditySingle(addressOrAlias: String, valid: Boolean, reason: Option[String])

  implicit val validitySingle: Format[ValiditySingle] = Json.format

  case class ValidityMany(validations: Seq[ValiditySingle])

  implicit val validityMany: Format[ValidityMany] = Json.format

  case class AddressScriptInfo(address: String, script: Option[String], scriptText: Option[String], complexity: Long)

  implicit val accountScriptInfoFormat: Format[AddressScriptInfo] = Json.format

  case class AddressPublicKeyInfo(address: String, publicKey: String)

  implicit val addressPublicKeyInfo: Format[AddressPublicKeyInfo] = Json.format
}
