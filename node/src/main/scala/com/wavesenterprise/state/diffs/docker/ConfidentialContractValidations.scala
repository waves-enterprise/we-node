package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.account.Address
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.transaction.docker.{
  ConfidentialDataInCreateContractSupported,
  ConfidentialDataInUpdateContractSupported,
  ExecutableTransaction
}
import com.wavesenterprise.transaction.{PaymentsV1ToContract, ValidationError}
import com.wavesenterprise.utils.ScorexLogging

object ConfidentialContractValidations extends ScorexLogging {
  private val MinimumGroupValidatorsSize: Int = 3

  private type ConfidentialCreateContractTx = ConfidentialDataInCreateContractSupported with PaymentsV1ToContract with ExecutableTransaction
  private type ConfidentialUpdateContractTx = ConfidentialDataInUpdateContractSupported with ExecutableTransaction

  def checkConfidentialCreateParamsSanity(confidentialContractTx: ConfidentialCreateContractTx,
                                          validators: Set[Address]): Either[ValidationError, Unit] = {
    def validatorsSize: Int      = validators.size
    def groupSize: Int           = confidentialContractTx.groupParticipants.size
    def groupValidatorsSize: Int = confidentialContractTx.groupParticipants.intersect(validators).size
    def groupOwnersSize: Int     = confidentialContractTx.groupOwners.size

    log.trace(
      s"Check confidential create params for tx '${confidentialContractTx.id()}':\n" +
        s"\tValidators – '${validators.mkString("', '")}'\n" +
        s"\tParticipants – '${confidentialContractTx.groupParticipants.mkString("', '")}'\n" +
        s"\tOwners – '${confidentialContractTx.groupOwners.mkString("', '")}'"
    )

    if (confidentialContractTx.isConfidential) {
      for {
        _ <- checkCommonForConfidentialTx(groupSize, groupValidatorsSize, groupOwnersSize, validatorsSize)
        _ <- Either.cond(
          !confidentialContractTx.isConfidential || confidentialContractTx.payments.isEmpty,
          (),
          ValidationError.GenericError(
            "Native token operations are not supported for confidential contracts, " +
              "so payment field must be empty when 'isConfidential' = true")
        )
      } yield ()
    } else {
      checkCommonForNonConfidentialTx(groupSize, groupOwnersSize)
    }
  }

  def checkConfidentialUpdateParamsSanity(confidentialUpdateContractTx: ConfidentialUpdateContractTx,
                                          contract: ContractInfo,
                                          validators: Set[Address]): Either[ValidationError, Unit] = {
    def validatorsSize: Int      = validators.size
    def groupSize: Int           = confidentialUpdateContractTx.groupParticipants.size
    def groupValidatorsSize: Int = confidentialUpdateContractTx.groupParticipants.intersect(validators).size
    def groupOwnersSize: Int     = confidentialUpdateContractTx.groupOwners.size

    log.trace(
      s"Check confidential update params for tx '${confidentialUpdateContractTx.id()}':\n" +
        s"\tValidators – '${validators.mkString("', '")}'\n" +
        s"\tParticipants – '${confidentialUpdateContractTx.groupParticipants.mkString("', '")}'\n" +
        s"\tOwners – '${confidentialUpdateContractTx.groupOwners.mkString("', '")}'"
    )

    if (contract.isConfidential) {
      checkCommonForConfidentialTx(groupSize, groupValidatorsSize, groupOwnersSize, validatorsSize)
    } else {
      checkCommonForNonConfidentialTx(groupSize, groupOwnersSize)
    }
  }

  private def checkCommonForConfidentialTx(groupSize: Int,
                                           groupValidatorsSize: Int,
                                           groupOwnersSize: Int,
                                           validatorsSize: Int): Either[ValidationError, Unit] = {
    for {
      _ <- Either.cond(
        groupOwnersSize > 0,
        (),
        ValidationError.GenericError(
          s"'groupOwners' field can't be empty list, it must contain at least 1 element")
      )

      _ <- Either.cond(
        groupValidatorsSize >= MinimumGroupValidatorsSize,
        (),
        ValidationError.GenericError(
          s"Not enough group participants with role 'contract-validator' to create confidential data contract, " +
            s"minimum is '$MinimumGroupValidatorsSize', actual: '$groupValidatorsSize'. Common validators size: '$validatorsSize'")
      )

      _ <- Either.cond(
        groupSize <= 1024 && groupOwnersSize <= 1024,
        (),
        ValidationError.GenericError(
          s"Maximum 'groupParticipants' and 'groupOwners' size is 1024, " +
            s"actual: len(groupParticipants) = $groupSize, len(groupOwners) = $groupOwnersSize")
      )
    } yield ()
  }

  private def checkCommonForNonConfidentialTx(groupSize: Int, groupOwnersSize: Int): Either[ValidationError, Unit] = {
    Either.cond(
      groupSize == 0 && groupOwnersSize == 0,
      (),
      ValidationError.GenericError("'groupParticipants' and 'groupOwners' fields can't be non-empty lists when 'isConfidential' = false")
    )
  }
}
