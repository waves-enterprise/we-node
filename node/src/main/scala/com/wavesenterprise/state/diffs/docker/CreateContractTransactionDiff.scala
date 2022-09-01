package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{Blockchain, Diff}
import com.wavesenterprise.transaction.ValidationError.UnexpectedTransactionError
import com.wavesenterprise.transaction.docker.CreateContractTransaction
import com.wavesenterprise.transaction.{Signed, ValidationError}

/**
  * Creates [[Diff]] for [[CreateContractTransaction]]
  */
case class CreateContractTransactionDiff(blockchain: Blockchain, blockOpt: Option[Signed], height: Int) extends ValidatorsValidator {

  def apply(tx: CreateContractTransaction): Either[ValidationError, Diff] = {
    blockOpt match {
      case Some(_) =>
        Left(UnexpectedTransactionError(tx))
      case None =>
        val contractInfo = ContractInfo(tx)

        checkValidators(contractInfo.validationPolicy).map { _ =>
          Diff(
            height = height,
            tx = tx,
            contracts = Map(contractInfo.contractId -> contractInfo),
            portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress, blockchain)
          )
        }
    }
  }
}
