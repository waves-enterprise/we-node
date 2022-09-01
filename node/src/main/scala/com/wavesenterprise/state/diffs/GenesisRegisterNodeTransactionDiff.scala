package com.wavesenterprise.state.diffs

import com.wavesenterprise.acl.OpType
import com.wavesenterprise.state.{Diff, ParticipantRegistration}
import com.wavesenterprise.transaction.{GenesisRegisterNodeTransaction, ValidationError}
import com.wavesenterprise.transaction.ValidationError.GenericError

object GenesisRegisterNodeTransactionDiff {
  def apply(height: Int)(tx: GenesisRegisterNodeTransaction): Either[ValidationError, Diff] = {
    if (height != 1) {
      Left(GenericError("GenesisRegisterNodeTransaction cannot appear in non-initial block"))
    } else {
      val registration = ParticipantRegistration(tx.targetPublicKey.toAddress, tx.targetPublicKey, OpType.Add)
      Right(Diff(height = height, tx = tx, registrations = Seq(registration)))
    }
  }
}
