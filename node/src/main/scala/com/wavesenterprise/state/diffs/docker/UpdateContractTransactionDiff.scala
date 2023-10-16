package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.{Blockchain, ContractId, Diff}
import com.wavesenterprise.transaction.ValidationError._
import com.wavesenterprise.transaction.docker.{ConfidentialDataInUpdateContractSupported, UpdateContractTransaction}
import com.wavesenterprise.transaction.{Signed, ValidationError, ValidationPolicyAndApiVersionSupport}

/**
 * Creates [[Diff]] for [[UpdateContractTransaction]]
 */
case class UpdateContractTransactionDiff(blockchain: Blockchain, blockOpt: Option[Signed], height: Int) extends ValidatorsValidator {

  def apply(tx: UpdateContractTransaction): Either[ValidationError, Diff] = {
    def checkBaseContractInfoChangesPermission(updateTx: UpdateContractTransaction,
                                               contract: ContractInfo): Either[ContractUpdateSenderError, Unit] = {
      val isParamsChanged = {
        val isImageParamsChanged = updateTx.image != contract.image || updateTx.imageHash != contract.imageHash
        val isValidationPolicyOrApiVersionChanged = updateTx match {
          case txValidationPolicyAndApiVersionSupported: ValidationPolicyAndApiVersionSupport =>
            txValidationPolicyAndApiVersionSupported.validationPolicy != contract.validationPolicy ||
              txValidationPolicyAndApiVersionSupported.apiVersion != contract.apiVersion
          case _ => false
        }

        isImageParamsChanged || isValidationPolicyOrApiVersionChanged
      }

      val isPermitted = updateTx.sender == contract.creator()

      Either.cond(isPermitted || !isParamsChanged, (), ContractUpdateSenderError(updateTx, contract.creator()))
    }

    def checkConfidentialContractInfoChangesPermission(updateTx: UpdateContractTransaction, contract: ContractInfo): Either[GenericError, Unit] = {
      val isParamsChanged = updateTx match {
        case confidentialDataTx: ConfidentialDataInUpdateContractSupported =>
          confidentialDataTx.groupParticipants != contract.groupParticipants ||
            confidentialDataTx.groupOwners != contract.groupOwners
        case _ => false
      }

      val senderAddress = updateTx.sender.toAddress
      val isSentByOwner = contract.groupOwners.contains(senderAddress)
      for {
        _ <- Either.cond(
          contract.isConfidential || !isParamsChanged,
          (),
          ValidationError.GenericError("Confidential contract's params updating is not allowed, " +
            "because confidential data is turned off('isConfidential' = false)")
        )
        _ <- Either.cond(
          isSentByOwner || !isParamsChanged,
          (),
          ValidationError.GenericError(s"Tx's sender '$senderAddress' doesn't belong to confidential contract group owners, " +
            "so it can't change related fields 'groupOwners' and 'groupParticipants")
        )
      } yield ()
    }

    def checkConfidentialDataTxParams(tx: UpdateContractTransaction, contract: ContractInfo): Either[ValidationError, Unit] = {
      tx match {
        case txWithConfidentialDataSupport: ConfidentialDataInUpdateContractSupported =>
          ConfidentialContractValidations.checkConfidentialUpdateParamsSanity(txWithConfidentialDataSupport,
                                                                              contract,
                                                                              blockchain.lastBlockContractValidators)
        case _ =>
          Right(())
      }
    }

    blockOpt match {
      case Some(_) => Left(UnexpectedTransactionError(tx))
      case None =>
        for {
          contractInfo <- blockchain.contract(ContractId(tx.contractId)).toRight(ContractNotFound(tx.contractId))
          _            <- checkBaseContractInfoChangesPermission(tx, contractInfo)
          _            <- checkConfidentialContractInfoChangesPermission(tx, contractInfo)
          _            <- checkConfidentialDataTxParams(tx, contractInfo)
          _            <- Either.cond(contractInfo.active, (), ContractIsDisabled(tx.contractId))
          updatedContractInfo = ContractInfo(tx, contractInfo)
          _ <- checkValidators(updatedContractInfo.validationPolicy) // todo: adapt to confidential smart contracts
        } yield Diff(
          height = height,
          tx = tx,
          contracts = Map(ContractId(tx.contractId) -> updatedContractInfo),
          portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain)
        )
    }
  }
}
