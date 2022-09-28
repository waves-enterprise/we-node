package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{Blockchain, Diff}
import com.wavesenterprise.transaction.ValidationError._
import com.wavesenterprise.transaction.docker.UpdateContractTransaction
import com.wavesenterprise.transaction.{Signed, ValidationError}

/**
  * Creates [[Diff]] for [[UpdateContractTransaction]]
  */
case class UpdateContractTransactionDiff(blockchain: Blockchain, blockOpt: Option[Signed], height: Int) extends ValidatorsValidator {

  def apply(tx: UpdateContractTransaction): Either[ValidationError, Diff] = {
    blockOpt match {
      case Some(_) => Left(UnexpectedTransactionError(tx))
      case None =>
        for {
          contractInfo <- blockchain.contract(tx.contractId).toRight(ContractNotFound(tx.contractId))
          _            <- Either.cond(contractInfo.active, (), ContractIsDisabled(tx.contractId))
          _            <- Either.cond(contractInfo.creator() == tx.sender, (), ContractUpdateSenderError(tx, contractInfo.creator()))
          updatedContractIndo = ContractInfo(tx, contractInfo)
          _ <- checkValidators(updatedContractIndo.validationPolicy)
        } yield
          Diff(
            height = height,
            tx = tx,
            contracts = Map(tx.contractId -> updatedContractIndo),
            portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain)
          )
    }
  }
}
