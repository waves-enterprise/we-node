package com.wavesenterprise.api.http.service

import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.{ContractNotFound, InvalidContractKeysFilter}
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId}
import cats.implicits._

trait ContractKeysOps {

  def blockchain: Blockchain

  protected def validateRegexKeysFilter(matches: Option[String]): Either[ApiError, Option[String => Boolean]] = {
    matches match {
      case None => Right(None)
      case Some(regex) =>
        Either
          .catchNonFatal(regex.r.pattern)
          .map(pattern => Some((key: String) => pattern.matcher(key).matches()))
          .leftMap(_ => InvalidContractKeysFilter(regex))
    }
  }

  protected def findContract(contractIdStr: String): Either[ContractNotFound, ContractInfo] = {
    for {
      contractId <- ByteStr.decodeBase58(contractIdStr).toEither.leftMap(_ => ContractNotFound(contractIdStr))
      contract   <- blockchain.contract(ContractId(contractId)).toRight(ContractNotFound(contractIdStr))
    } yield contract
  }
}
