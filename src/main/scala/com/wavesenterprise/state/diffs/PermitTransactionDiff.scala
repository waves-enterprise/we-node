package com.wavesenterprise.state.diffs

import cats.syntax.either._
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.{NonEmptyRole, OpType, Permissions, Role}
import com.wavesenterprise.features.BlockchainFeature.ContractValidationsSupport
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.settings.GenesisSettings
import com.wavesenterprise.state.{Blockchain, Diff, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{AddressIsLastOfRole, GenericError}
import com.wavesenterprise.transaction.acl.PermitTransaction

import scala.annotation.tailrec
import scala.util.Try

case class PermitTransactionDiff(blockchain: Blockchain, genesisSettings: GenesisSettings, blockTimestamp: Long, height: Int) {

  def apply(tx: PermitTransaction): Either[ValidationError, Diff] = {
    for {
      _             <- checkRemoveWithDueDate(tx)
      _             <- checkTimestamp(tx)
      _             <- checkSenderRole(tx)
      targetAddress <- blockchain.resolveAlias(tx.target)
      _             <- checkScripted(targetAddress)
      _             <- checkDuplicateAction(tx, targetAddress)
      _             <- checkLastOfRole(tx, targetAddress)
      _             <- checkContractValidationActive(tx)
    } yield
      Diff(
        height,
        tx,
        portfolios = Map(tx.sender.toAddress -> Portfolio(-tx.fee, LeaseBalance.empty, Map.empty)),
        permissions = Map(targetAddress      -> Permissions(Seq(tx.permissionOp)))
      )
  }

  private def checkSenderRole(tx: PermitTransaction): Either[GenericError, Unit] = {
    Either.cond(
      tx.permissionOp.role != Role.Sender || genesisSettings.senderRoleEnabled,
      (),
      GenericError("The role 'Sender' is not allowed by network parameters")
    )
  }

  private def checkScripted(targetAddress: Address): Either[ValidationError, Unit] =
    Either.cond(!blockchain.hasScript(targetAddress), (), GenericError(s"Account $targetAddress is scripted, therefore not allowed to have roles"))

  private def checkTimestamp(tx: PermitTransaction): Either[ValidationError, Unit] =
    Either.cond(tx.permissionOp.dueTimestampOpt.forall(_ > blockTimestamp),
                (),
                GenericError("Cannot add permission transaction that is already expired"))

  private def checkDuplicateAction(tx: PermitTransaction, targetAddress: Address): Either[ValidationError, Unit] = {
    Try(blockchain.permissions(targetAddress).lastActive(tx.permissionOp.role, blockTimestamp))
      .fold(ex => Left(GenericError(ex)), Right.apply)
      .flatMap { maybeLastActivePermissionOp =>
        (tx.permissionOp.opType, maybeLastActivePermissionOp) match {
          case (OpType.Add, Some(activePermOp)) =>
            if (activePermOp.dueTimestampOpt.isEmpty && tx.permissionOp.dueTimestampOpt.isEmpty)
              Left(GenericError("Cannot assign role that is already active"))
            else
              Right(())

          case (OpType.Remove, None) =>
            Left(GenericError("Cannot remove role that is already inactive"))

          case (_, _) =>
            Right(())
        }
      }
  }

  private def checkLastOfRole(tx: PermitTransaction, targetAddress: Address): Either[ValidationError, Unit] = {
    @tailrec
    def isNotLast(unresolvedRoles: List[NonEmptyRole], addresses: => Stream[Address]): Either[NonEmptyRole, Unit] = {
      (unresolvedRoles, addresses) match {
        case (Nil, _)                            => Right(())
        case (unresolvedRole :: _, Stream.Empty) => Left(unresolvedRole)
        case (_, address #:: remainingAddresses) =>
          val remainingRoles = unresolvedRoles.filterNot { role =>
            val permissions = blockchain.permissions(address)
            permissions.containsWithoutDueDate(role, blockTimestamp) &&
            !permissions.contains(Role.Banned, blockTimestamp)
          }

          isNotLast(remainingRoles, remainingAddresses)
      }
    }

    def allAddressesWithoutTarget: Stream[Address] = blockchain.allNonEmptyRoleAddresses.filter(_ != targetAddress)

    ((tx.permissionOp.opType, tx.permissionOp.role) match {
      case (OpType.Add, Role.Banned) =>
        val activeNonEmptyRoles = blockchain.permissions(targetAddress).active(blockTimestamp).collect {
          case role: NonEmptyRole => role
        }

        isNotLast(activeNonEmptyRoles.toList, allAddressesWithoutTarget)
      case (OpType.Remove, targetRole: NonEmptyRole) =>
        isNotLast(targetRole :: Nil, allAddressesWithoutTarget)
      case _ => Right(())
    }).leftMap(AddressIsLastOfRole(targetAddress, _))
  }

  private def checkRemoveWithDueDate(tx: PermitTransaction): Either[ValidationError, Unit] = {
    (tx.permissionOp.opType, tx.permissionOp.dueTimestampOpt) match {
      case (OpType.Remove, Some(_)) =>
        Left(GenericError("Due date should not be defined for PermitTransaction with OpType Remove"))
      case _ =>
        Right(())
    }
  }

  private def checkContractValidationActive(tx: PermitTransaction): Either[ValidationError, Unit] = {
    tx.permissionOp.role match {
      case Role.ContractValidator =>
        Either.cond(
          blockchain.isFeatureActivated(ContractValidationsSupport, height),
          (),
          GenericError(s"Role '${Role.ContractValidator}' will be available after contract validations feature activation")
        )
      case _ =>
        Right(())
    }
  }
}
