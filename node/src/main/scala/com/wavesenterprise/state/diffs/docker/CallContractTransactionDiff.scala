package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{Blockchain, Diff}
import com.wavesenterprise.transaction.ValidationError.{ContractIsDisabled, ContractNotFound, ContractVersionMatchError, UnexpectedTransactionError}
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CallContractTransactionV1}
import com.wavesenterprise.transaction.{Signed, ValidationError}

/**
  * Creates [[Diff]] for [[CallContractTransaction]]
  */
case class CallContractTransactionDiff(blockchain: Blockchain, blockOpt: Option[Signed], height: Int) extends ValidatorsValidator {

  def apply(tx: CallContractTransaction): Either[ValidationError, Diff] =
    blockOpt match {
      case Some(_) => Left(UnexpectedTransactionError(tx))
      case None =>
        for {
          contractInfo <- blockchain.contract(tx.contractId).toRight(ContractNotFound(tx.contractId))
          _            <- checkContractVersion(tx, contractInfo)
          _            <- checkValidators(contractInfo.validationPolicy)
          _            <- Either.cond(contractInfo.active, (), ContractIsDisabled(tx.contractId))
        } yield Diff(height = height, tx = tx, portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress, blockchain))
    }

  private def checkContractVersion(tx: CallContractTransaction, ci: ContractInfo): Either[ValidationError, Unit] = {
    tx match {
      case _: CallContractTransactionV1 =>
        Either.cond(ci.version == ContractInfo.FirstVersion, (), ContractVersionMatchError(ci, ContractInfo.FirstVersion))
      case ctxV2 =>
        Either.cond(ci.version == ctxV2.contractVersion, (), ContractVersionMatchError(ci, ctxV2.contractVersion))
    }
  }
}
