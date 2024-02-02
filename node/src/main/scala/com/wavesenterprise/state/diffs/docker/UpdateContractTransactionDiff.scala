package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.{Blockchain, ContractId, Diff}
import com.wavesenterprise.transaction.ValidationError._
import com.wavesenterprise.transaction.docker.{
  ConfidentialDataInUpdateContractSupported,
  DockerContractTransaction,
  UpdateContractTransaction,
  UpdateContractTransactionV6
}
import com.wavesenterprise.transaction.{ApiVersionSupport, Signed, ValidationError, ValidationPolicySupport}

/**
  * Creates [[Diff]] for [[UpdateContractTransaction]]
 **/
case class UpdateContractTransactionDiff(blockchain: Blockchain, blockOpt: Option[Signed], height: Int) extends ValidatorsValidator
    with BytecodeValidator {
  def apply(tx: UpdateContractTransaction): Either[ValidationError, Diff] = {
    def checkBaseContractInfoChangesPermission(updateTx: UpdateContractTransaction,
                                               contract: ContractInfo): Either[ContractUpdateSenderError, Unit] = {
      val isContractChanged = {
        updateTx match {
          case tx: UpdateContractTransactionV6 =>
            tx.storedContract != contract.storedContract
          case tx: DockerContractTransaction =>
            DockerContract(tx.image, tx.imageHash) != contract.storedContract
        }
      }

      val isParamsChanged = {
        val isValidationPolicyOrApiVersionChanged = updateTx match {
          case txValidationPolicyAndApiVersionSupported: ValidationPolicySupport with ApiVersionSupport =>
            txValidationPolicyAndApiVersionSupported.validationPolicy != contract.validationPolicy ||
              txValidationPolicyAndApiVersionSupported.apiVersion != contract.apiVersion
          case _ => false
        }
        isContractChanged || isValidationPolicyOrApiVersionChanged
      }

      val isPermitted = updateTx.sender == contract.creator()

      Either.cond(
        isPermitted || !isParamsChanged,
        (),
        ContractUpdateSenderError(updateTx, contract.creator())
      )
    }

    def checkContractInfoEngineChanged(updateTx: UpdateContractTransaction, contract: ContractInfo): Either[ValidationError, Unit] = {
      val contractNotChanged = {
        updateTx match {
          case tx: UpdateContractTransactionV6 =>
            tx.storedContract.engine() == contract.storedContract.engine()
          case tx: DockerContractTransaction =>
            contract.storedContract.engine() == "docker"
        }
      }
      Either.cond(
        contractNotChanged,
        (),
        GenericError(s"changed engine in $updateTx")
      )
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
          _            <- checkContractInfoEngineChanged(tx, contractInfo)
          _            <- checkConfidentialContractInfoChangesPermission(tx, contractInfo)
          _            <- checkConfidentialDataTxParams(tx, contractInfo)
          _            <- Either.cond(contractInfo.active, (), ContractIsDisabled(tx.contractId))
          _            <- Either.cond(contractInfo.creator() == tx.sender, (), ContractUpdateSenderError(tx, contractInfo.creator()))
          updatedContractInfo = ContractInfo(tx, contractInfo)
          _ <- checkValidators(updatedContractInfo.validationPolicy)
          _ <- checkBytecode(updatedContractInfo)
        } yield Diff(
          height = height,
          tx = tx,
          contracts = Map(ContractId(tx.contractId) -> updatedContractInfo),
          portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain)
        )
    }
  }
}
