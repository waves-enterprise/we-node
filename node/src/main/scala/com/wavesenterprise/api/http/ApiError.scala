package com.wavesenterprise.api.http

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import com.wavesenterprise.account.{Address, AddressOrAlias, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.NonEmptyRole
import com.wavesenterprise.api.http.auth.AuthRole
import com.wavesenterprise.crypto.internals.{
  CryptoError,
  DecryptionError => CryptoDecryptionError,
  GenericError => CryptoGenericError,
  InvalidAddress => CryptoInvalidAddress,
  InvalidPublicKey => CryptoInvalidPublicKey,
  PkiError => CryptoPkiError
}
import com.wavesenterprise.database.snapshot.SnapshotStatus
import com.wavesenterprise.lang.ExprEvaluator.Log
import com.wavesenterprise.lang.v1.evaluator.ctx.LazyVal
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.privacy.db.DBError
import com.wavesenterprise.privacy.s3.{BucketError, InvalidHash, ParseError, S3Error}
import com.wavesenterprise.settings.NodeMode
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.state.diffs.TransactionDiffer.TransactionValidationError
import com.wavesenterprise.transaction.{Transaction, ValidationError}
import enumeratum.values.{IntEnum, IntEnumEntry}
import play.api.libs.json._

import scala.collection.immutable

case class ApiErrorResponse(error: Int, message: String)

object ApiErrorResponse {
  implicit val toFormat: Reads[ApiErrorResponse] = Json.reads[ApiErrorResponse]
}

trait ApiErrorBase {
  def id: Int
  val message: String
  val code: StatusCode

  lazy val json: JsObject = Json.obj("error" -> id, "message" -> message)
}

sealed trait ApiError extends IntEnumEntry with ApiErrorBase {
  def id: Int = value
}

object ApiError extends IntEnum[ApiError] {
  def fromDBError(e: DBError): ApiError = e match {
    case DBError.DuplicateKey                             => EntityAlreadyExists
    case validationErrorWrap: DBError.ValidationErrorWrap => ApiError.fromValidationError(validationErrorWrap.validationError)
    case DBError.UnmappedError                            => GeneralDBError
    case _                                                => GeneralDBError
  }

  def fromS3Error(e: S3Error): ApiError = e match {
    case ParseError(message)           => CustomValidationError(message)
    case BucketError(message)          => CustomValidationError(message)
    case InvalidHash(actual, expected) => CustomValidationError(s"Invalid data hash. Actual '$actual', expected '$expected'")
    case _                             => GeneralDBError
  }

  def fromValidationError(e: ValidationError): ApiError = {
    e match {
      case ValidationError.NegativeAmount(x, of)    => NegativeAmount(s"$x of $of")
      case ValidationError.NegativeMinFee(x, of)    => NegativeMinFee(s"$x per $of")
      case ValidationError.InvalidAddress(reason)   => InvalidAddress(reason)
      case ValidationError.InvalidPublicKey(reason) => InvalidPublicKey(reason)
      case ValidationError.InvalidName              => InvalidName
      case ValidationError.InvalidSignature(_, _)   => InvalidSignature
      case ValidationError.InvalidRequestSignature  => InvalidSignature
      case ValidationError.TooBigArray              => TooBigArrayAllocation
      case ValidationError.OverflowError            => OverflowError
      case ValidationError.MissingSenderPrivateKey  => MissingSenderPrivateKey
      case ValidationError.GenericError(ge)         => CustomValidationError(ge)
      case ValidationError.AlreadyInTheState(tx, txHeight) =>
        CustomValidationError(s"Transaction $tx is already in the state on a height of $txHeight")

      case ValidationError.BalanceErrors(accountErrs, contractErrs) =>
        val accountErrorsStr = Option(accountErrs)
          .filter(_.nonEmpty)
          .map(nonEmptyErrors => s"Account errors: ${nonEmptyErrors.mkString(", ")}")

        val contractErrorsStr: Option[String] = Option(contractErrs)
          .filter(_.nonEmpty)
          .map(nonEmptyErrors => s"Contract errors: ${nonEmptyErrors.mkString(", ")}")

        CustomValidationError(List(accountErrorsStr, contractErrorsStr).flatten.mkString("; "))
      case ValidationError.AliasDoesNotExist(tx)      => AliasDoesNotExist(tx)
      case ValidationError.OrderValidationError(_, m) => CustomValidationError(m)
      case ValidationError.UnsupportedTransactionType => CustomValidationError("UnsupportedTransactionType")
      case ValidationError.Mistiming(err)             => Mistiming(err)
      case TransactionValidationError(error, tx) =>
        error match {
          case ValidationError.Mistiming(errorMessage) => Mistiming(errorMessage)
          case ValidationError.TransactionNotAllowedByScript(vars, scriptSrc, isTokenScript) =>
            TransactionNotAllowedByScript(tx, vars, scriptSrc, isTokenScript)
          case ValidationError.ScriptExecutionError(err, src, vars, isToken) =>
            ScriptExecutionError(tx, err, src, vars, isToken)
          case _: ValidationError.ContractError                   => fromValidationError(error)
          case _: ValidationError.PrivacyError                    => fromValidationError(error)
          case ValidationError.AddressIsLastOfRole(address, role) => AddressIsLastOfRole(tx, address, role)
          case _                                                  => StateCheckFailed(tx, fromValidationError(error).message)
        }
      case ValidationError.ContractNotFound(contractId)                    => ContractNotFound(contractId.toString)
      case ValidationError.ContractIsDisabled(contractId)                  => ContractIsDisabled(contractId.toString)
      case ValidationError.ContractAlreadyDisabled(contractId)             => ContractAlreadyDisabled(contractId.toString)
      case ValidationError.ContractTransactionTooBig(wrongSize, rightSize) => ContractTransactionTooBig(wrongSize, rightSize)
      case ValidationError.ParticipantNotRegistered(address)               => ParticipantNotRegistered(address)
      case ValidationError.PolicyDataTooBig(policySize, maxSize)           => PolicyDataTooBig(policySize, maxSize)
      case ValidationError.PolicyDoesNotExist(policyId)                    => PolicyDoesNotExist(policyId)
      case ValidationError.PolicyDataHashAlreadyExists(hash)               => PolicyDataHashAlreadyExists(hash)
      case ValidationError.ReachedSnapshotHeightError(height)              => ReachedSnapshotHeightError(height)
      case ValidationError.InvalidAssetId(message)                         => InvalidAssetId(message)
      case ValidationError.UnsupportedContractApiVersion(_, err)           => UnsupportedContractApiVersion(err)
      case ValidationError.InvalidContractApiVersion(err)                  => InvalidContractApiVersion(err)
      case ValidationError.CertificateNotFound(pk)                         => CertificateNotFound(pk)
      case ValidationError.CertificatePathBuildError(pk, err)              => CertificatePathBuildError(pk, err)
      case cpe: ValidationError.CertificateParseError                      => CertificateParseError(cpe.toString)
      case error                                                           => CustomValidationError(error.toString)
    }
  }

  def fromCryptoError(e: CryptoError): ApiError = {
    e match {
      case CryptoInvalidAddress(reason)     => InvalidAddress(reason)
      case CryptoInvalidPublicKey(reason)   => InvalidPublicKey(reason)
      case CryptoDecryptionError(reason, _) => CustomValidationError(reason)
      case CryptoGenericError(reason)       => CustomValidationError(reason)
      case CryptoPkiError(reason)           => CustomValidationError(reason)
    }
  }

  implicit val lvWrites: Writes[LazyVal] = Writes { lv =>
    lv.value.value.attempt
      .map({
        case Left(thr) =>
          Json.obj(
            "status" -> "Failed",
            "error"  -> thr.getMessage
          )
        case Right(Left(err)) =>
          Json.obj(
            "status" -> "Failed",
            "error"  -> err
          )
        case Right(Right(lv)) =>
          Json.obj(
            "status" -> "Success",
            "value"  -> lv.toString
          )
      })()
  }

  case object Unknown extends ApiError {
    override val value   = 0
    override val code    = StatusCodes.InternalServerError
    override val message = "Error is unknown"
  }

  case class WrongJson(cause: Option[Throwable] = None, errors: Seq[(JsPath, Seq[JsonValidationError])] = Seq.empty) extends ApiError {
    override val value        = 1
    override val code         = StatusCodes.BadRequest
    override lazy val message = "failed to parse json message"
    override lazy val json: JsObject = Json.obj(
      "error"            -> id,
      "message"          -> message,
      "cause"            -> cause.map(_.toString),
      "validationErrors" -> JsError.toJson(errors)
    )
  }

  //API Auth
  case object ApiKeyNotValid extends ApiError {
    override val value           = 2
    override val code            = StatusCodes.Forbidden
    override val message: String = "Provided API key is not correct"
  }

  case object DiscontinuedApi extends ApiError {
    override val value   = 3
    override val code    = StatusCodes.BadRequest
    override val message = "This API is no longer supported"
  }

  case object TooBigArrayAllocation extends ApiError {
    override val value: Int       = 10
    override val message: String  = "Too big sequences requested"
    override val code: StatusCode = StatusCodes.BadRequest
  }

  //VALIDATION
  case object InvalidSignature extends ApiError {
    override val value   = 101
    override val code    = StatusCodes.BadRequest
    override val message = "invalid signature"
  }

  case class InvalidAddress(reason: String) extends ApiError {
    override val value   = 102
    override val code    = StatusCodes.BadRequest
    override val message = s"invalid address: $reason"
  }

  case object InvalidSeed extends ApiError {
    override val value   = 103
    override val code    = StatusCodes.BadRequest
    override val message = "invalid seed"
  }

  case object InvalidAmount extends ApiError {
    override val value   = 104
    override val code    = StatusCodes.BadRequest
    override val message = "invalid amount"
  }

  case object InvalidFee extends ApiError {
    override val value   = 105
    override val code    = StatusCodes.BadRequest
    override val message = "invalid fee"
  }

  case object InvalidSender extends ApiError {
    override val value   = 106
    override val code    = StatusCodes.BadRequest
    override val message = "invalid sender"
  }

  case object InvalidRecipient extends ApiError {
    override val value   = 107
    override val code    = StatusCodes.BadRequest
    override val message = "invalid recipient"
  }

  case class InvalidPublicKey(reason: String) extends ApiError {
    override val value   = 108
    override val code    = StatusCodes.BadRequest
    override val message = s"invalid public key: $reason"
  }

  case object InvalidNotNumber extends ApiError {
    override val value   = 109
    override val code    = StatusCodes.BadRequest
    override val message = "argument is not a number"
  }

  case object InvalidMessage extends ApiError {
    override val value   = 110
    override val code    = StatusCodes.BadRequest
    override val message = "invalid message"
  }

  case class NegativeAmount(msg: String) extends ApiError {
    override val value: Int       = 111
    override val message: String  = s"negative amount: $msg"
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case class StateCheckFailed(tx: Transaction, err: String) extends ApiError {
    override val value: Int       = 112
    override val message: String  = s"State check failed. Reason: $err"
    override val code: StatusCode = StatusCodes.BadRequest
    override lazy val json        = Json.obj("error" -> id, "message" -> message, "tx" -> tx.json())
  }

  case object OverflowError extends ApiError {
    override val value: Int       = 113
    override val message: String  = "overflow error"
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case class NegativeMinFee(msg: String) extends ApiError {
    override val value: Int       = 114
    override val message: String  = s"negative fee per: $msg"
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case object MissingSenderPrivateKey extends ApiError {
    override val value: Int       = 115
    override val message: String  = "no private key for sender address in wallet or provided password is incorrect"
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case object InvalidName extends ApiError {
    override val value: Int       = 116
    override val message: String  = "invalid name"
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case class AddressIsLastOfRole(tx: Transaction, address: Address, role: NonEmptyRole) extends ApiError {
    override val value: Int       = 117
    override val message: String  = s"Trying to revoke role '$role' from it's last owner: '$address'"
    override val code: StatusCode = StatusCodes.BadRequest
    override lazy val json        = Json.obj("error" -> id, "message" -> message, "tx" -> tx.json())
  }

  case class InvalidAssetId(message: String) extends ApiError {
    override val value: Int       = 118
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case class CertificateNotFound(keyName: String, keyValue: String) extends ApiError {
    override val value: Int       = 119
    override val code: StatusCode = StatusCodes.NotFound
    override val message: String  = s"Couldn't find a certificate with $keyName '$keyValue'"
  }

  object CertificateNotFound {
    def apply(pk: PublicKeyAccount): CertificateNotFound = {
      CertificateNotFound("public key", pk.publicKeyBase58)
    }
  }

  case class CertificatePathBuildError(publicKey: PublicKeyAccount, reason: String) extends ApiError {
    override val value: Int       = 120
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String = {
      s"Error while validating provided certificate chain for the public key '${publicKey.publicKeyBase58}': $reason"
    }
  }

  case class CertificateParseError(reason: String) extends ApiError {
    override val value: Int       = 121
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = s"Error while parsing a certificate: $reason"
  }

  case class CustomValidationError(errorMessage: String) extends ApiError {
    override val value: Int       = 199
    override val message: String  = errorMessage
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case class BlockDoesNotExist(signatureOrHeight: Either[ByteStr, Int]) extends ApiError {
    override val value: Int      = 301
    override val code            = StatusCodes.NotFound
    override val message: String = s"block ${signatureOrHeight.fold(s => s"with signature '$s'", h => s"at height '$h'")} does not exist"
  }

  case class AliasDoesNotExist(aoa: AddressOrAlias) extends ApiError {
    override val value: Int = 302
    override val code       = StatusCodes.NotFound
    private lazy val msgReason = aoa match {
      case a: Address => s"for address '${a.stringRepr}'"
      case a: Alias   => s"'${a.stringRepr}'"
    }
    override val message: String = s"alias $msgReason doesn't exist"
  }

  case class Mistiming(errorMessage: String) extends ApiError {
    override val value: Int       = 303
    override val message: String  = errorMessage
    override val code: StatusCode = StatusCodes.BadRequest
  }

  case object DataKeyNotExists extends ApiError {
    override val value: Int      = 304
    override val code            = StatusCodes.NotFound
    override val message: String = "no data for this key"
  }

  case class ScriptCompilerError(errorMessage: String) extends ApiError {
    override val value: Int       = 305
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = errorMessage
  }

  case class ScriptExecutionError(tx: Transaction, error: String, scriptSrc: String, log: Log, isTokenScript: Boolean) extends ApiError {
    override val value: Int          = 306
    override val code: StatusCode    = StatusCodes.BadRequest
    override val message: String     = s"Error while executing ${if (isTokenScript) "token" else "account"}-script: $error"
    override lazy val json: JsObject = ScriptErrorJson(id, tx, message, scriptSrc, log)

  }

  case class TransactionNotAllowedByScript(tx: Transaction, log: Log, scriptSrc: String, isTokenScript: Boolean) extends ApiError {

    override val value: Int          = 307
    override val code: StatusCode    = StatusCodes.BadRequest
    override val message: String     = s"Transaction is not allowed by ${if (isTokenScript) "token" else "account"}-script"
    override lazy val json: JsObject = ScriptErrorJson(id, tx, message, scriptSrc, log)
  }

  case class SignatureError(error: String) extends ApiError {
    override val value: Int       = 309
    override val code: StatusCode = StatusCodes.InternalServerError
    override val message: String  = s"Signature error: $error"
  }

  case class RequestedHeightDoesntExist(requestedHeight: Int, currentHeight: Int) extends ApiError {
    override val value: Int       = 310
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = s"Requested height '$requestedHeight' is greater than current state height '$currentHeight'"
  }

  object ScriptErrorJson {
    def apply(errId: Int, tx: Transaction, message: String, scriptSrc: String, log: Log): JsObject =
      Json.obj(
        "error"       -> errId,
        "message"     -> message,
        "transaction" -> tx.json(),
        "script"      -> scriptSrc,
        "vars" -> Json.arr(log.map {
          case (k, Right(v))  => Json.obj("name" -> k, "value" -> JsString(v.toString))
          case (k, Left(err)) => Json.obj("name" -> k, "error" -> JsString(err))
        })
      )
  }

  case class ContractNotFound(contractId: String) extends ApiError {
    override val value: Int                    = 600
    override val code: StatusCodes.ClientError = StatusCodes.NotFound
    override val message: String               = s"Contract '$contractId' is not found"
  }

  case class ExecutedTransactionNotFound(forTxId: String) extends ApiError {
    override val value: Int                    = 601
    override val code: StatusCodes.ClientError = StatusCodes.NotFound
    override val message: String               = s"Executed transaction is not found for transaction with txId = $forTxId"
  }

  case class ContractIsDisabled(contractId: String) extends ApiError {
    override val value: Int                    = 602
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"Contract '$contractId' has been disabled. You cannot call disabled contract"
  }

  case class ContractAlreadyDisabled(contractId: String) extends ApiError {
    override val value: Int                    = 603
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"Contract '$contractId' has been already disabled"
  }

  case class ContractTransactionTooBig(wrongSize: Long, rightSize: Long) extends ApiError {
    override val value: Int                    = 604
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"Contract transaction too big, actual size $wrongSize bytes, expected size $rightSize bytes"
  }

  case class ContractExecutionNotFound(txId: ByteStr) extends ApiError {
    override val value: Int       = 605
    override val code: StatusCode = StatusCodes.NotFound
    override val message: String  = s"Contract execution result is not found for transaction with txId = '$txId'"
  }

  case class ParticipantNotRegistered(unregisteredAddress: Address) extends ApiError {
    override val value: Int       = 606
    override val code: StatusCode = StatusCodes.Unauthorized
    override val message: String  = s"Address '${unregisteredAddress.stringRepr}' does not have access to the network"
  }

  case class PolicyDataTooBig(policySize: Long, maxSize: Long) extends ApiError {
    override val value: Int                    = 607
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"Policy data too big, actual size '$policySize' bytes, max size '$maxSize' bytes"
  }

  //Database
  case object EntityAlreadyExists extends ApiError {
    override val value: Int       = 608
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = "Inserting entity that already exists"
  }

  case object GeneralDBError extends ApiError {
    override val value: Int       = 609
    override val code: StatusCode = StatusCodes.InternalServerError
    override val message: String  = "DB error"
  }

  case object ForceSyncError extends ApiError {
    override val value: Int       = 610
    override val code: StatusCode = StatusCodes.InternalServerError
    override val message: String  = "Force sync error"
  }

  case class HttpEntityTooBig(entitySize: Long, maxSize: Long) extends ApiError {
    override val value: Int       = 611
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = s"HttpEntity too big, actual size '$entitySize' bytes, max size '$maxSize' bytes"
  }

  case class PolicyDoesNotExist(policyId: ByteStr) extends ApiError {
    override val value: Int                    = 612
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"The requested policy $policyId does not exist"
  }

  case object PrivacyIsSwitchedOff extends ApiError {
    override val value: Int                    = 613
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = "The privacy feature is switched off"
  }

  case object PrivacyApiKeyNotValid extends ApiError {
    override val value: Int                    = 614
    override val code: StatusCodes.ClientError = StatusCodes.Forbidden
    override val message: String               = "Provided privacy API key is not correct"
  }

  case class PolicyDataHashAlreadyExists(hash: PolicyDataHash) extends ApiError {
    override val value: Int                    = 615
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"The specified dataset with hash $hash was added earlier"
  }

  case object PolicyDataAlreadyIsWritten extends ApiError {
    override val value: Int                    = 616
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = "The data is written to the node. In the process of creating a confirmation transaction..."
  }

  case class PolicyItemDataIsMissing(hash: String) extends ApiError {
    override val value: Int                    = 617
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"The requested dataset $hash is missing in privacy storage"
  }

  /**
    * Authorization related errors
    **/
  case object MissingAuthorizationMetadata extends ApiError {
    override val value: Int                    = 618
    override val code: StatusCodes.ClientError = StatusCodes.Unauthorized
    override val message: String               = "Authorization error. Missing authorization metadata"
  }

  case object AuthTokenExpired extends ApiError {
    override val value: Int                    = 619
    override val code: StatusCodes.ClientError = StatusCodes.Unauthorized
    override val message: String               = "Authorization error. Token expired"
  }

  case object CantParseJwtClaims extends ApiError {
    override val value: Int                    = 620
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"Authorization error. Can't parse JWT token claims"
  }

  case class NoRequiredAuthRole(role: AuthRole) extends ApiError {
    override val value: Int                    = 621
    override val code: StatusCodes.ClientError = StatusCodes.Forbidden
    override val message: String               = s"Authorization error. No expected role '${role}' "
  }

  case object InvalidTokenError extends ApiError {
    override val value: Int                    = 622
    override val code: StatusCodes.ClientError = StatusCodes.Unauthorized
    override val message: String               = s"Authorization error. Invalid token"
  }

  /**
    * Contracts API
    */
  case class InvalidContractKeysFilter(keysFilter: String) extends ApiError {
    override val value: Int                    = 629
    override val code: StatusCodes.ClientError = StatusCodes.BadRequest
    override val message: String               = s"Keys filter parameter has invalid regex pattern '$keysFilter'"
  }

  case object InvalidNodeOwnerAddress extends ApiError {
    override val value: Int                    = 636
    override val code: StatusCodes.ClientError = StatusCodes.Forbidden
    override val message: String               = "Authorization error. Invalid node owner address"
  }

  case class ServiceIsDisabled(serviceName: String) extends ApiError {
    override val value: Int                    = 637
    override val code: StatusCodes.ClientError = StatusCodes.NotFound
    override val message: String               = s"Service '$serviceName' is disabled"
  }

  case object IllegalWatcherActionError extends ApiError {
    override val value: Int                    = 638
    override val code: StatusCodes.ClientError = StatusCodes.Forbidden
    override val message: String               = s"Sending transactions or data is disabled for node in '${NodeMode.Watcher}' mode"
  }

  case class HealthCheckError(originMessage: String) extends ApiError {
    override val value: Int       = 640
    override val code: StatusCode = StatusCodes.ServiceUnavailable
    override val message: String  = originMessage
  }

  case class UnimplementedError(description: String) extends ApiError {
    override val value: Int                    = 641
    override val code: StatusCodes.ServerError = StatusCodes.NotImplemented
    override val message: String               = description
  }

  case class TooManyConnections(maxConnectionsCount: Int) extends ApiError {
    override val value: Int       = 642
    override val code: StatusCode = StatusCodes.ServiceUnavailable
    override val message: String  = s"Max connections count '$maxConnectionsCount' has been exceeded. Please, try again later"
  }

  case class InvalidConnectionId(originMessage: String) extends ApiError {
    override val value: Int       = 643
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = originMessage
  }

  case class UnsupportedContractApiVersion(err: String) extends ApiError {
    override val value: Int       = 644
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = err
  }

  case class InvalidContractApiVersion(err: String) extends ApiError {
    override val value: Int       = 645
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = err
  }

  case class PolicyItemNotFound(policyId: String, itemId: String) extends ApiError {
    override val value: Int       = 646
    override val code: StatusCode = StatusCodes.NotFound
    override val message: String  = s"Policy $policyId item $itemId not found in database"
  }

  case class WrongPolicyItemDataGetMethod(err: String) extends ApiError {
    override val value: Int       = 647
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = err
  }

  case object PrivacyLargeObjectFeatureIsNotActivated extends ApiError {
    override val value: Int       = 648
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String = "Large privacy object feature is not activated yet, " +
      "so using appropriate api methods(for getting or sending large data) is not allowed"
  }

  /**
    * Snapshot feature range (700-799)
    */
  case class SnapshotNotVerified(status: SnapshotStatus) extends ApiError {
    override val value: Int                    = 700
    override val code: StatusCodes.ClientError = StatusCodes.NotFound
    override val message: String               = s"Snapshot isn't yet verified. Current status '${status.name}'. Try later"
  }

  case object SnapshotGenesisNotFound extends ApiError {
    override val value: Int                    = 701
    override val code: StatusCodes.ServerError = StatusCodes.InternalServerError
    override val message: String               = "Genesis block is not found in snapshot. Probably snapshot state is invalid"
  }

  case object SnapshotFeatureDisabled extends ApiError {
    override val value: Int       = 702
    override val code: StatusCode = StatusCodes.Forbidden
    override val message: String  = "Snapshot feature is disabled"
  }

  case class ReachedSnapshotHeightError(snapshotHeight: Int) extends ApiError {
    override val value: Int       = 703
    override val code: StatusCode = StatusCodes.Forbidden
    override val message: String  = s"Snapshot height '$snapshotHeight' is reached. Unable to process transactions"
  }

  case object SnapshotAlreadySwapped extends ApiError {
    override val value: Int       = 704
    override val code: StatusCode = StatusCodes.BadRequest
    override val message: String  = "Snapshot has been already swapped"
  }

  case class SnapshotFileSystemError(reason: String) extends ApiError {
    override val value: Int       = 705
    override val code: StatusCode = StatusCodes.InternalServerError
    override val message: String  = s"Couldn't process snapshot because of error: '$reason'"
  }

  override def values: immutable.IndexedSeq[ApiError] = findValues
}
