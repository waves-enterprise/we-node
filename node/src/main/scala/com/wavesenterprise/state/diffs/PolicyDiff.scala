package com.wavesenterprise.state.diffs

import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.state.{Blockchain, ByteStr, PrivacyBlockchain}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{GenericError, PolicyDataHashAlreadyExists, PolicyDoesNotExist}

object PolicyDiff {

  def checkPolicyIdEmptiness(policyId: ByteStr): Either[ValidationError, Unit] = {
    if (policyId.arr.isEmpty) {
      Left(GenericError(s"PolicyId could not be empty"))
    } else {
      Right(())
    }
  }

  def checkPolicyExistence(policyId: ByteStr, blockchain: Blockchain): Either[ValidationError, Unit] = {
    Either.cond(blockchain.policyExists(policyId), (), PolicyDoesNotExist(policyId))
  }

  def checkPolicyDataHashNotExists(policyId: ByteStr,
                                   dataHash: PolicyDataHash,
                                   blockchain: PrivacyBlockchain): Either[PolicyDataHashAlreadyExists, Unit] =
    Either.cond(!blockchain.policyDataHashExists(policyId, dataHash), (), PolicyDataHashAlreadyExists(dataHash))

  def checkPolicyNotExists(policyId: ByteStr, blockchain: Blockchain): Either[ValidationError, Unit] = {
    checkPolicyExistence(policyId, blockchain) match {
      case Right(_) => Left(GenericError("Policy already exists"))
      case Left(_)  => Right(())
    }
  }
}
