package com.wavesenterprise.mining

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.docker.validator.ContractValidatorResultsStore
import com.wavesenterprise.network.{ContractValidatorResults, ContractValidatorResultsV2}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.docker.{ExecutedContractTransactionV4, ValidationProof}
import com.wavesenterprise.utils.{ScorexLogging, Time}

trait ConfidentialUnpermittedSetupProcess extends ContractValidatorResultsOps with ScorexLogging {

  def contractValidatorResultsStoreOpt: Option[ContractValidatorResultsStore]

  def keyBlockId: ByteStr

  def ownerKey: PrivateKeyAccount

  def time: Time

  def blockchain: Blockchain

  def transactionsAccumulator: TransactionsAccumulator

  private val minerAddress = ownerKey.toAddress

  def processConfidentialExecutableUnpermittedSetup(setup: ConfidentialExecutableUnpermittedSetup): Either[ValidationError, TransactionWithDiff] = {
    val ConfidentialExecutableUnpermittedSetup(tx, contractInfo, maybeCertChainWithCrl) = setup

    val validationPolicy = contractInfo.validationPolicy
    val validators       = blockchain.lastBlockContractValidators - minerAddress
    val txId             = tx.id()

    for {
      contractValidatorResults   <- selectContractValidatorResults(txId, validators, validationPolicy)
      contractValidatorResultsV2 <- checkContractValidatorResultsV2Presense(txId, contractValidatorResults)
      bestGroup =
        contractValidatorResultsV2
          .groupBy(r => r.txId -> r.resultsHash -> r.readings -> r.readingsHash -> r.outputCommitment).values.maxBy(_.size)
      validationProofs = bestGroup.map(proof => ValidationProof(proof.sender, proof.signature))
      contractValidatorResult <- getContractResult(bestGroup, txId)
      executedContractTxV4 <- ExecutedContractTransactionV4.selfSigned(
        sender = ownerKey,
        tx = tx,
        results = List.empty,
        resultsHash = contractValidatorResult.resultsHash,
        validationProofs = validationProofs,
        timestamp = time.getTimestamp(),
        assetOperations = List.empty,
        readings = contractValidatorResult.readings.toList,
        readingsHash = contractValidatorResult.readingsHash,
        outputCommitment = contractValidatorResult.outputCommitment
      )
      _ =
        log.debug(s"Built executed transaction '${executedContractTxV4.id()}' for '${executedContractTxV4.tx.id()}' from contract validator results")

      diff <- transactionsAccumulator.process(
        executedContractTxV4,
        /*
          when miner is not included to confidential group participants
          we have not access to confidential data, so cannot build confidential output
         */
        maybeConfidentialOutput = None,
        maybeCertChainWithCrl = maybeCertChainWithCrl
      )
    } yield TransactionWithDiff(executedContractTxV4, diff)

  }

  private def getContractResult(bestGroup: List[ContractValidatorResultsV2], txId: ByteStr): Either[ValidationError, ContractValidatorResultsV2] =
    bestGroup.headOption.toRight(ValidationError.GenericError(s"Contract validator results for tx: '$txId' are not found"))

  override def collectContractValidatorResults(keyBlockId: ByteStr,
                                               txId: ByteStr,
                                               validators: Set[Address],
                                               resultHash: Option[ByteStr] = None,
                                               limit: Option[Int] = None): List[ContractValidatorResults] =
    contractValidatorResultsStoreOpt
      .map(_.findResults(keyBlockId, txId, validators, requiredResultHash = None, limit = limit)
        .collect { case resultsV2: ContractValidatorResultsV2 => resultsV2 })
      .toList
      .flatten

  private def checkContractValidatorResultsV2Presense(
      txId: ByteStr,
      contractValidatorResults: List[ContractValidatorResults]): Either[ValidationError, List[ContractValidatorResultsV2]] = {
    val contractValidatorResultsV2 = contractValidatorResults
      .collect { case r: ContractValidatorResultsV2 => r }
    if (contractValidatorResultsV2.isEmpty) {
      Left(ValidationError.GenericError(s"Empty contract validator results for transaction id: $txId"))
    } else Right(contractValidatorResultsV2)
  }
}
