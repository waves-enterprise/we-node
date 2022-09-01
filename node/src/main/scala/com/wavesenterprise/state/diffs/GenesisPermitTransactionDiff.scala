package com.wavesenterprise.state.diffs

import com.wavesenterprise.acl.{OpType, PermissionOp, Permissions, Role}
import com.wavesenterprise.settings.GenesisSettings
import com.wavesenterprise.state.Diff
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{GenesisPermitTransaction, ValidationError}

object GenesisPermitTransactionDiff {
  def apply(genesisSettings: GenesisSettings, height: Int)(tx: GenesisPermitTransaction): Either[ValidationError, Diff] =
    for {
      _ <- checkHeight(height)
      _ <- checkSenderRole(tx, genesisSettings)
    } yield {
      val permOp = PermissionOp(OpType.Add, tx.role, tx.timestamp, dueTimestampOpt = None)
      Diff(height = height, tx = tx, permissions = Map(tx.target -> Permissions(Seq(permOp))))
    }

  private def checkHeight(height: Int): Either[GenericError, Unit] = {
    Either.cond(height == 1, (), GenericError("GenesisPermitTransaction cannot appear in non-initial block"))
  }

  private def checkSenderRole(tx: GenesisPermitTransaction, genesisSettings: GenesisSettings): Either[GenericError, Unit] = {
    Either.cond(
      tx.role != Role.Sender || genesisSettings.senderRoleEnabled,
      (),
      GenericError("The role 'Sender' is not allowed by network parameters")
    )
  }
}
