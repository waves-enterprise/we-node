package com.wavesenterprise.mining

import com.wavesenterprise.account.Address
import com.wavesenterprise.docker.validator.{ContractValidatorResultsStore, ValidationPolicy}
import com.wavesenterprise.network.ContractValidatorResults
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.InvalidValidationProofs

trait ContractValidatorResultsOps {

  def contractValidatorResultsStoreOpt: Option[ContractValidatorResultsStore]

  def keyBlockId: ByteStr

  protected def collectContractValidatorResults(keyBlockId: ByteStr,
                                                txId: ByteStr,
                                                validators: Set[Address],
                                                resultHash: Option[ByteStr] = None,
                                                limit: Option[Int] = None): List[ContractValidatorResults] = List.empty[ContractValidatorResults]

  protected def selectContractValidatorResults(txId: ByteStr,
                                               validators: Set[Address],
                                               validationPolicy: ValidationPolicy,
                                               resultsHash: Option[ByteStr] = None): Either[ValidationError, List[ContractValidatorResults]] = {
    validationPolicy match {
      case ValidationPolicy.Any => Right(List.empty)
      case ValidationPolicy.Majority =>
        val majoritySize                            = math.ceil(validators.size * ValidationPolicy.MajorityRatio).toInt
        val results: List[ContractValidatorResults] = collectContractValidatorResults(keyBlockId, txId, validators, resultsHash, Some(majoritySize))

        Either.cond(
          results.size >= majoritySize,
          results,
          InvalidValidationProofs(results.size, majoritySize, validators, resultsHash = None)
        )
      case ValidationPolicy.MajorityWithOneOf(addresses) =>
        val requiredAddresses = addresses.toSet
        val majoritySize      = math.ceil(validators.size * ValidationPolicy.MajorityRatio).toInt
        val validatorResults  = collectContractValidatorResults(keyBlockId, txId, validators)

        val (results, resultsSize, resultsContainsRequiredAddresses) = validatorResults
          .foldLeft((List.empty[ContractValidatorResults], 0, false)) {
            case ((acc, accSize, accContainsRequired), i) =>
              if (accContainsRequired && accSize >= majoritySize)
                (acc, accSize, accContainsRequired)
              else if (!accContainsRequired && accSize >= majoritySize && requiredAddresses.contains(i.sender.toAddress))
                (i :: acc.tail, accSize, true)
              else
                (i :: acc, accSize + 1, accContainsRequired || requiredAddresses.contains(i.sender.toAddress))
          }

        Either.cond(
          resultsContainsRequiredAddresses && resultsSize >= majoritySize,
          results,
          InvalidValidationProofs(resultsSize, majoritySize, validators, resultsHash, resultsContainsRequiredAddresses, requiredAddresses)
        )
    }
  }
    .map(_.reverse)
}
