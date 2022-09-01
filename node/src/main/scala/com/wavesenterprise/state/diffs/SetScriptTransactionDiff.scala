package com.wavesenterprise.state.diffs

import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.lang.v1.DenyDuplicateVarNames
import com.wavesenterprise.state.{Blockchain, Diff, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptValidation}
import com.wavesenterprise.utils.SmartContractV1Utils._

import scala.util.Right

case class SetScriptTransactionDiff(blockchain: Blockchain, height: Int) {
  def apply(tx: SetScriptTransaction): Either[ValidationError, Diff] = {
    for {
      _ <- checkScriptVersion(tx)
      _ <- Either.cond(
        blockchain.permissions(tx.sender.toAddress).active(tx.timestamp).isEmpty,
        (),
        GenericError("Script cannot be assigned to an account with active roles!")
      )
      _ <- tx.script.fold(Right(()): Either[ValidationError, Unit]) { script =>
        if (blockchain.isFeatureActivated(BlockchainFeature.SmartAccountTrading, height)) {
          Right(())
        } else {
          val version = script.version
          DenyDuplicateVarNames(version, varNames(version), script.expr).left.map(GenericError.apply)
        }
      }
    } yield {
      Diff(
        height = height,
        tx = tx,
        portfolios = Map(tx.sender.toAddress -> Portfolio(-tx.fee, LeaseBalance.empty, Map.empty)),
        scripts = Map(tx.sender.toAddress    -> tx.script)
      )
    }
  }

  def checkScriptVersion(tx: SetScriptTransaction): Either[ValidationError, Unit] = {
    tx.script.map(_.version) match {
      case Some(scriptVersion) =>
        Either.cond(
          SetScriptValidation.SupportedScriptVersions.contains(scriptVersion),
          (),
          GenericError(s"Bad script version '${scriptVersion.value}'")
        )
      case None =>
        Right(())
    }
  }
}
