package com.wavesenterprise.state

import com.wavesenterprise.account.Address
import com.wavesenterprise.consensus.ContractValidatorPool
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.transaction.ValidationError.ContractNotFound
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.{AssetId, ValidationPolicyAndApiVersionSupport, ValidationError}

/**
  * Blockchain with smart contract transactions.
  */
trait ContractBlockchain {

  def contracts(): Set[ContractInfo]

  def contract(contractId: ByteStr): Option[ContractInfo]

  def contractKeys(request: KeysRequest, readingContext: ContractReadingContext): Vector[String]

  def contractData(contractId: ByteStr, readingContext: ContractReadingContext): ExecutedContractData

  def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]]

  def contractData(contractId: ByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData

  def executedTxFor(forTxId: ByteStr): Option[ExecutedContractTransaction]

  def hasExecutedTxFor(forTxId: ByteStr): Boolean

  def contractBalanceSnapshots(contractId: ByteStr, from: Int, to: Int): Seq[BalanceSnapshot]

  def contractBalance(contractId: AssetId, maybeAssetId: Option[AssetId], readingContext: ContractReadingContext): Long

  def contractPortfolio(contractId: ByteStr): Portfolio

  def contractValidators: ContractValidatorPool

  def lastBlockContractValidators: Set[Address]

  def validationPolicy(tx: ExecutableTransaction): Either[ValidationError, ValidationPolicy] =
    tx match {
      case createTxWithValidationPolicy: CreateContractTransaction with ValidationPolicyAndApiVersionSupport =>
        Right(createTxWithValidationPolicy.validationPolicy)
      case _: CreateContractTransaction =>
        Right(ValidationPolicy.Default)
      case _ =>
        contract(tx.contractId)
          .map(_.validationPolicy)
          .toRight(ContractNotFound(tx.contractId))
    }

}

object ContractBlockchain {

  sealed trait ContractReadingContext

  object ContractReadingContext {
    case object Default                            extends ContractReadingContext
    case class TransactionExecution(txId: ByteStr) extends ContractReadingContext
  }
}
