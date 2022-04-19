package com.wavesenterprise.state.diffs.docker

import cats.implicits._
import cats.kernel.Monoid
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.network.ContractValidatorResults
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff}
import com.wavesenterprise.transaction.ValidationError.{ContractNotFound, GenericError, InvalidSender}
import com.wavesenterprise.transaction.docker.{ExecutedContractTransactionV2, _}
import com.wavesenterprise.transaction.{Signed, ValidationError}

/**
  * Creates [[Diff]] for [[ExecutedContractTransaction]]
  */
case class ExecutedContractTransactionDiff(
    blockchain: Blockchain,
    blockTimestamp: Long,
    height: Int,
    blockOpt: Option[Signed],
    minerOpt: Option[PublicKeyAccount]
) {

  def apply(tx: ExecutedContractTransaction): Either[ValidationError, Diff] =
    checkExecutableTxIsNotExecuted(tx) >>
      ((blockOpt, minerOpt) match {
        case (Some(block), _)    => applyInner(tx, block.sender)
        case (None, Some(miner)) => applyInner(tx, miner)
        case _                   => Left(GenericError("Block or microblock is needed to validate this transaction"))
      })

  private def applyInner(executedTx: ExecutedContractTransaction, miner: PublicKeyAccount): Either[ValidationError, Diff] =
    validateMinerIsSender(miner, executedTx) >>
      checkResultsHash(executedTx) >>
      checkValidationProofs(executedTx, miner.toAddress) >>
      calcDiff(executedTx)

  private def calcDiff(executedTx: ExecutedContractTransaction): Either[ValidationError, Diff] = {
    executedTx.tx match {
      case tx: CreateContractTransaction =>
        val contractInfo = ContractInfo(tx)
        val contractData = ExecutedContractData(executedTx.results.map(v => (v.key, v)).toMap)
        CreateContractTransactionDiff(blockchain, None, height)(tx).map { innerDiff =>
          Monoid.combine(
            innerDiff,
            Diff(
              height = height,
              tx = executedTx,
              contractsData = Map(contractInfo.contractId -> contractData),
              executedTxMapping = Map(tx.id()             -> executedTx.id())
            )
          )
        }

      case tx: CallContractTransaction =>
        val contractData = ExecutedContractData(executedTx.results.map(v => (v.key, v)).toMap)
        CallContractTransactionDiff(blockchain, None, height)(tx).map { innerDiff =>
          Monoid.combine(innerDiff,
                         Diff(
                           height = height,
                           tx = executedTx,
                           contractsData = Map(tx.contractId -> contractData),
                           executedTxMapping = Map(tx.id()   -> executedTx.id())
                         ))
        }

      case tx: UpdateContractTransaction =>
        for {
          _         <- blockchain.contract(tx.contractId).toRight(ContractNotFound(tx.contractId))
          innerDiff <- UpdateContractTransactionDiff(blockchain, None, height)(tx)
        } yield
          Monoid.combine(
            innerDiff,
            Diff(
              height = height,
              tx = executedTx,
              executedTxMapping = Map(tx.id() -> executedTx.id())
            )
          )

      case _ => Left(ValidationError.GenericError("Unknown type of transaction"))
    }
  }

  private def validateMinerIsSender(miner: PublicKeyAccount, tx: ExecutedContractTransaction): Either[ValidationError, Unit] = {
    Either.cond(
      miner == tx.sender,
      (),
      InvalidSender(
        s"ExecutedContractTransaction should be created only by block creator: tx sender is '${tx.sender.address}', but block creator is '${miner.toAddress}'")
    )
  }

  private def checkExecutableTxIsNotExecuted(ect: ExecutedContractTransaction): Either[ValidationError, Unit] = {
    Either.cond(!blockchain.hasExecutedTxFor(ect.tx.id()),
                (),
                ValidationError.GenericError(s"Executable transaction ${ect.tx} has been already executed"))
  }

  private def checkResultsHash(tx: ExecutedContractTransaction) = {
    tx match {
      case _: ExecutedContractTransactionV1 => Right(())
      case tx: ExecutedContractTransactionV2 =>
        val expectedHash = ContractValidatorResults.resultsHash(tx.results)
        Either.cond(
          tx.resultsHash == expectedHash,
          (),
          ValidationError.InvalidResultsHash(tx.resultsHash, expectedHash)
        )
    }
  }

  private def checkValidationProofs(tx: ExecutedContractTransaction, minerAddress: Address): Either[ValidationError, Unit] = {
    tx match {
      case _: ExecutedContractTransactionV1 => Right(())
      case tx: ExecutedContractTransactionV2 =>
        for {
          validationPolicy <- blockchain.validationPolicy(tx.tx)
          _ <- validationPolicy match {
            case ValidationPolicy.Any                          => Right(())
            case ValidationPolicy.Majority                     => checkValidationProofMajority(tx, minerAddress)
            case ValidationPolicy.MajorityWithOneOf(addresses) => checkValidationProofMajority(tx, minerAddress, addresses.toSet)
          }
          _ <- tx.validationProofs.traverse(checkValidationProof(tx.resultsHash, _))
        } yield ()
    }
  }

  private def checkValidationProofMajority(tx: ExecutedContractTransactionV2,
                                           minerAddress: Address,
                                           requiredAddresses: Set[Address] = Set.empty): Either[ValidationError, Unit] = {
    val validators     = blockchain.lastBlockContractValidators - minerAddress
    val proofAddresses = tx.validationProofs.view.map(_.validatorPublicKey.toAddress).toSet
    val filteredCount  = proofAddresses.count(validators.contains)
    val majoritySize   = math.ceil(ValidationPolicy.MajorityRatio * validators.size).toInt

    val requiredAddressesCondition = requiredAddresses.isEmpty || (requiredAddresses intersect proofAddresses).nonEmpty
    val majorityCondition          = filteredCount >= majoritySize

    Either.cond(
      requiredAddressesCondition && majorityCondition,
      (),
      ValidationError.InvalidValidationProofs(filteredCount, majoritySize, validators, tx.resultsHash, requiredAddressesCondition, requiredAddresses)
    )
  }

  private def checkValidationProof(resultsHash: ByteStr,
                                   validationProof: ValidationProof): Either[ValidationError.InvalidValidatorSignature, Unit] = {
    Either.cond(
      crypto.verify(validationProof.signature.arr, resultsHash.arr, validationProof.validatorPublicKey.publicKey),
      (),
      ValidationError.InvalidValidatorSignature(validationProof.validatorPublicKey, validationProof.signature)
    )
  }
}
