package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.ValidationError

trait ValidatorsValidator {

  def blockchain: Blockchain

  def checkValidators(validationPolicy: ValidationPolicy): Either[ValidationError, Unit] =
    validationPolicy match {
      case ValidationPolicy.Any =>
        Right(())
      case ValidationPolicy.Majority =>
        Either.cond(blockchain.lastBlockContractValidators.nonEmpty, (), ValidationError.NotEnoughValidators())
      case ValidationPolicy.MajorityWithOneOf(addresses) =>
        val addressesSet = addresses.toSet
        val isValid      = blockchain.lastBlockContractValidators.intersect(addressesSet).nonEmpty
        Either.cond(isValid, (), ValidationError.NotEnoughValidators(addressesSet))
    }
}
