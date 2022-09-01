package com.wavesenterprise.transaction.validation

import cats.implicits._
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.state.{ByteStr, ContractBlockchain}
import com.wavesenterprise.transaction.ValidationError.{ContractNotFound, UnsupportedContractApiVersion}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction, ValidationError}

import scala.util.Right

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
          .contract(callTx.contractId)
          .map(_.apiVersion)
          .orElse {
            atomicTransactions.collectFirst {
              case ccv4: CreateContractTransactionV4 if ccv4.contractId == callTx.contractId => ccv4.apiVersion
              case cc: CreateContractTransaction if cc.contractId == callTx.contractId       => ContractApiVersion.`1.0`
            }
          }
          .toRight[ValidationError](ContractNotFound(callTx.contractId)) >>= (isApiVersionSupported(callTx.contractId, _))
      case ccv4: CreateContractTransactionV4 => isApiVersionSupported(ccv4.contractId, ccv4.apiVersion)
      case ucv4: UpdateContractTransactionV4 => isApiVersionSupported(ucv4.contractId, ucv4.apiVersion)
      case _                                 => Right(())
    }).map(_ => tx)
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
