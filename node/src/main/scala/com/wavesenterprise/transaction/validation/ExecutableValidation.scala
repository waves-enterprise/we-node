package com.wavesenterprise.transaction.validation

import cats.implicits._
import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.docker.{ContractApiVersion, StoredContract}
import com.wavesenterprise.state.{ByteStr, ContractBlockchain, ContractId}
import com.wavesenterprise.transaction.ValidationError.{ContractNotFound, UnsupportedContractApiVersion}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.{ApiVersionSupport, AtomicTransaction, StoredContractSupported, Transaction, ValidationError}

object ExecutableValidation {
  def validateApiVersion(
      tx: Transaction,
      blockchain: ContractBlockchain,
      atomicTransactions: Seq[Transaction] = Seq.empty
  ): Either[ValidationError, Transaction] = {
    (tx match {
      case atomic: AtomicTransaction => atomic.transactions.traverse(validateApiVersion(_, blockchain, atomic.transactions)).as(atomic)
      case callTx: CallContractTransaction =>
        blockchain
          .contract(ContractId(callTx.contractId))
          .flatMap(info => getApiVersion(info.storedContract))
          .orElse {
            atomicTransactions.collectFirst {
              case createWithValidation: CreateContractTransaction with ApiVersionSupport
                  if createWithValidation.contractId == callTx.contractId =>
                createWithValidation.apiVersion
              case cc: CreateContractTransaction if cc.contractId == callTx.contractId => ContractApiVersion.`1.0`
            }
          }
          .toRight[ValidationError](ContractNotFound(callTx.contractId))

      case v7: ExecutableTransaction with StoredContractSupported if v7.storedContract.engine() == "docker" =>
        isApiVersionSupported(v7.contractId, getApiVersion(v7.storedContract).get)
      case createWithValidation: CreateContractTransaction with ApiVersionSupport =>
        isApiVersionSupported(createWithValidation.contractId, createWithValidation.apiVersion)
      case updateWithValidation: UpdateContractTransaction with ApiVersionSupport =>
        isApiVersionSupported(updateWithValidation.contractId, updateWithValidation.apiVersion)
      case _ => Right(())
    }).map(_ => tx)
  }

  def getApiVersion(contract: StoredContract): Option[ContractApiVersion] = {
    contract match {
      case DockerContract(_, _, apiVersion) => Some(apiVersion)
      case _                                => None
    }
  }

  private def isApiVersionSupported(contractId: ByteStr, apiVersion: ContractApiVersion): Either[ValidationError, Unit] = {
    Either.cond(
      apiVersion.majorVersion == ContractApiVersion.Current.majorVersion && apiVersion.minorVersion <= ContractApiVersion.Current.minorVersion,
      (),
      UnsupportedContractApiVersion(
        contractId.base58,
        s"Unsupported contract API version. Current version: '${ContractApiVersion.Current}'. Got: '$apiVersion'"
      )
    )
  }
}
