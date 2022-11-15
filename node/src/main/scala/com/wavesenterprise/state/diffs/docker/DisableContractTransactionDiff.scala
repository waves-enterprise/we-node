package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{Blockchain, ContractId, Diff}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{ContractAlreadyDisabled, ContractNotFound, GenericError}
import com.wavesenterprise.transaction.docker.DisableContractTransaction

/**
  * Creates [[Diff]] for [[DisableContractTransaction]]
  */
case class DisableContractTransactionDiff(blockchain: Blockchain, height: Int) {

  private def validateSenderIsDeveloper(ci: ContractInfo, tx: DisableContractTransaction): Either[ValidationError, Diff] = {
    Either.cond(
      ci.creator() == tx.sender,
      Diff.empty,
      GenericError(s"Can't disable contract: transaction sender '${tx.sender}' is not developer of contract '${ci.contractId}'")
    )
  }

  def apply(tx: DisableContractTransaction): Either[ValidationError, Diff] = {
    blockchain
      .contract(ContractId(tx.contractId))
      .map(ci =>
        validateSenderIsDeveloper(ci, tx).flatMap(_ =>
          Either.cond(
            ci.active,
            Diff(
              height = height,
              tx = tx,
              portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain),
              contracts = Map(ContractId(tx.contractId) -> ci.copy(active = false))
            ),
            ContractAlreadyDisabled(tx.contractId)
        )))
      .getOrElse(Left(ContractNotFound(tx.contractId)))
  }
}
