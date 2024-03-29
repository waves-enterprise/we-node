package com.wavesenterprise.api.http.service

import cats.implicits._
import com.wavesenterprise.api.http.ApiError._
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.http.service.ContractsApiService.{BalanceDetails, ContractAssetBalanceInfo}
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.docker.{ContractExecutionMessage, ContractExecutionMessagesCache, ContractInfo}
import com.wavesenterprise.settings.Constants
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId, DataEntry}
import com.wavesenterprise.transaction.ValidationError.{GenericError, InvalidContractKeys}
import com.wavesenterprise.utils.StringUtilites.ValidateAsciiAndRussian.notValidMapOrRight
import monix.reactive.Observable
import play.api.libs.json.{Format, JsObject, Json}

import scala.util.{Failure, Success, Try}

class ContractsApiService(
    override val blockchain: Blockchain,
    messagesCache: ContractExecutionMessagesCache,
) extends ContractKeysOps {

  def contracts(): Set[ContractInfo] = {
    blockchain.contracts()
  }

  def contractKeys(contractsIdColl: Iterable[String],
                   readingContext: ContractReadingContext): Either[ApiError, Map[String, Iterable[DataEntry[_]]]] = {
    contractsIdColl.toVector
      .traverse(findContract)
      .map { contracts =>
        contractsData(contracts, readingContext)
      }
  }

  def validateAllowReadContractKeys(contractId: String, requestingContractId: ContractId): Either[ApiError, Unit] = {
    findContract(contractId) match {
      case Right(contractInfo) if !contractInfo.isConfidential                            => Right(())
      case Right(contractInfo) if contractInfo.contractId == requestingContractId.byteStr => Right(())
      case Right(_) => Left(ApiError.CustomValidationError(
          s"Only confidential contract itself is allowed to get confidential keys: requesting contract id -> '$requestingContractId', required contract id -> '$contractId'"
        ))
      case error => error.map(_ => ())
    }
  }

  def contractKeys(contractId: String,
                   offsetOpt: Option[Int],
                   limitOpt: Option[Int],
                   matches: Option[String],
                   readingContext: ContractReadingContext): Either[ApiError, Vector[DataEntry[_]]] = {
    validateRegexKeysFilter(matches).flatMap { keysFilter =>
      contractKeysWithFilter(contractId, offsetOpt, limitOpt, keysFilter, readingContext)
    }
  }

  def contractKeys(contractIdStr: String, keys: Iterable[String], readingContext: ContractReadingContext): Either[ApiError, Vector[DataEntry[_]]] = {
    findContract(contractIdStr).map { contract =>
      blockchain.contractData(contract.contractId, keys, readingContext).toVector
    }
  }

  private def contractKeysWithFilter(contractIdStr: String,
                                     offsetOpt: Option[Int],
                                     limitOpt: Option[Int],
                                     keysFilter: Option[String => Boolean],
                                     readingContext: ContractReadingContext): Either[ApiError, Vector[DataEntry[_]]] = {
    for {
      contract <- findContract(contractIdStr)
      keysRequest = KeysRequest(contract.contractId, offsetOpt, limitOpt, keysFilter)
      keys        = blockchain.contractKeys(keysRequest, readingContext)
    } yield blockchain.contractData(contract.contractId, keys, readingContext).toVector
  }

  def contractInfo(contractId: String): Either[ContractNotFound, ContractInfo] = {
    findContract(contractId)
  }

  def contractKey(contractId: String, key: String, readingContext: ContractReadingContext): Either[ApiError, DataEntry[_]] =
    for {
      _         <- notValidMapOrRight(key).leftMap(InvalidContractKeys.apply).leftMap(fromValidationError)
      contract  <- findContract(contractId)
      dataEntry <- blockchain.contractData(contract.contractId, key, readingContext).toRight(DataKeyNotExists(key))
    } yield dataEntry

  def executedTransactionFor(transactionId: String): Either[ApiError, JsObject] = {
    ByteStr.decodeBase58(transactionId) match {
      case Success(txId) =>
        blockchain.executedTxFor(txId) match {
          case Some(tx) => Right(tx.json())
          case None     => Left(ExecutedTransactionNotFound(transactionId))
        }
      case Failure(_) => Left(ApiError.fromValidationError(GenericError(s"Failed to decode base58 transaction id value '$transactionId'")))
    }
  }

  def executionStatuses(transactionId: String): Either[ApiError, Seq[ContractExecutionMessage]] = {
    ByteStr.decodeBase58(transactionId) match {
      case Success(txId) => executionStatuses(txId)
      case Failure(_)    => Left(ApiError.fromValidationError(GenericError(s"Failed to decode base58 transaction id value '$transactionId'")))
    }
  }

  def executionStatuses(transactionId: ByteStr): Either[ApiError, Seq[ContractExecutionMessage]] = {
    messagesCache.get(transactionId) match {
      case Some(messages) => Right(messages.toSeq.sortBy(_.timestamp))
      case None           => Left(ContractExecutionNotFound(transactionId))
    }
  }

  def lastMessage: Observable[ContractExecutionMessage] = messagesCache.lastMessage

  def contractAssetBalance(contractId: String,
                           assetId: Option[String],
                           readingContext: ContractReadingContext): Either[ApiError, ContractAssetBalanceInfo] = {
    import cats.implicits._

    for {
      contractIdByteStr <- ByteStr
        .decodeBase58(contractId)
        .toEither
        .leftMap(_ => ApiError.CustomValidationError(s"Failed to decode base58 contract id value '$contractId'"))
      maybeAssetIdByteStr <- assetId
        .map(ByteStr.decodeBase58)
        .sequence[Try, ByteStr]
        .toEither
        .leftMap(_ => ApiError.CustomValidationError(s"Failed to decode base58 asset id value '${assetId.getOrElse("")}'"))
      decimals <- maybeAssetIdByteStr
        .map(
          blockchain
            .assetDescription(_)
            .toRight(ApiError.InvalidAssetId(s"Unable to find a description for AssetId '$assetId'"))
            .map(_.decimals.toInt))
        .getOrElse(Right(Constants.WestDecimals))
    } yield ContractAssetBalanceInfo(blockchain.contractBalance(ContractId(contractIdByteStr), maybeAssetIdByteStr, readingContext), decimals)
  }

  def contractBalanceDetails(contractIdStr: String): Either[ApiError, BalanceDetails] = {

    for {
      contractIdByteStr <- ByteStr
        .decodeBase58(contractIdStr)
        .toEither
        .leftMap(_ => ApiError.CustomValidationError(s"Failed to decode base58 contract id value '$contractIdStr'"))
      portfolio = blockchain.contractWestPortfolio(ContractId(contractIdByteStr))
    } yield BalanceDetails(
      contractId = contractIdStr,
      regular = portfolio.balance,
      leasedOut = portfolio.lease.out,
      available = portfolio.spendableBalance
    )

  }

  private def contractsData(contracts: Iterable[ContractInfo], readingContext: ContractReadingContext): Map[String, Iterable[DataEntry[_]]] = {
    (for {
      contract <- contracts
      data = blockchain.contractData(contract.contractId, readingContext).data.values
    } yield contract.contractId.toString -> data).toMap
  }

}

object ContractsApiService {

  case class ContractAssetBalanceInfo(amount: Long, decimals: Int)

  case class BalanceDetails(contractId: String, regular: Long, leasedOut: Long, available: Long)

  implicit val balanceDetailsFormat: Format[BalanceDetails] = Json.format

}
