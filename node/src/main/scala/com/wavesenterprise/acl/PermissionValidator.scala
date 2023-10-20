package com.wavesenterprise.acl

import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.Role._
import com.wavesenterprise.settings.GenesisSettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.ValidationError.PermissionError
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.assets.exchange.ExchangeTransaction
import com.wavesenterprise.transaction.assets.{IssueTransaction, ReissueTransaction, SetAssetScriptTransactionV1}
import com.wavesenterprise.transaction.docker.{CreateContractTransaction, UpdateContractTransaction}

import scala.util.Try

trait PermissionValidator {
  def validatePermissionForTx(blockchain: Blockchain, tx: Transaction): Either[PermissionError, Unit]
  def validateMiner(blockchain: Blockchain, address: Address, onTimestamp: Long): Either[PermissionError, Unit]
}

private class PermissionValidatorImpl(senderRoleEnabled: Boolean) extends PermissionValidator {

  import PermissionValidatorImpl._

  def validatePermissionForTx(blockchain: Blockchain, tx: Transaction): Either[PermissionError, Unit] = tx match {
    case _: GenesisTransaction | _: GenesisPermitTransaction | _: GenesisRegisterNodeTransaction =>
      Right(())

    case authorizedTx: Transaction with Authorized =>
      val senderAccount = authorizedTx.sender
      (for {
        permissions <- tryAsEither(blockchain.permissions(senderAccount.toAddress))
        _           <- checkAddressBanned(permissions, authorizedTx.timestamp)
        _           <- senderRequirement.validate(permissions, tx.timestamp)
        _           <- specificRoleRequirementForTx(tx).validate(permissions, tx.timestamp)
      } yield ()).left
        .map(permErr => PermissionError(s"Sender ${senderAccount.address} permission validation failed: ${permErr.err}"))

    case unauthorizedTx =>
      Left(PermissionError(s"Transaction ${unauthorizedTx.id()} is unauthorized, permission validation impossible."))
  }

  def validateMiner(blockchain: Blockchain, address: Address, onTimestamp: Long): Either[PermissionError, Unit] =
    for {
      permissions <- tryAsEither(blockchain.permissions(address))
      _           <- checkAddressBanned(permissions, onTimestamp)
      isMiner     <- tryAsEither(permissions.contains(Role.Miner, onTimestamp))
      _           <- Either.cond(isMiner, (), PermissionError(s"Address ${address.address} is not a miner"))
    } yield ()

  private def senderRequirement: RequireRole = {
    if (senderRoleEnabled)
      RequireSingle(Sender)
    else
      RequireNone
  }

  private def specificRoleRequirementForTx(tx: Transaction): RequireRole = tx match {
    case permitTx: PermitTransaction if permitTx.permissionOp.role == Banned =>
      RequireAnyOf(Permissioner :: Blacklister :: Nil)
    case _: PermitTransaction =>
      RequireSingle(Permissioner)
    case _: IssueTransaction | _: ReissueTransaction =>
      RequireSingle(Issuer)
    case _: ExchangeTransaction =>
      RequireSingle(Dexer)
    case _: SetAssetScriptTransactionV1 =>
      RequireSingle(ContractDeveloper)
    case _: RegisterNodeTransaction =>
      RequireSingle(ConnectionManager)
    case _: CreateContractTransaction =>
      RequireSingle(ContractDeveloper)
    case _: UpdateContractTransaction =>
      RequireSingle(ContractDeveloper)
    case _ =>
      RequireNone
  }

  private def checkAddressBanned(permissions: Permissions, timestamp: Long): Either[PermissionError, Unit] =
    RequireNot(Banned).validate(permissions, timestamp)

  private def tryAsEither[T](f: => T): Either[PermissionError, T] =
    Try(f).fold(
      ex => Left(PermissionError(s"Blockchain error during permission validation: $ex")),
      result => Right(result)
    )
}

object PermissionValidatorImpl {
  private sealed trait RequireRole {
    def validate(permissions: Permissions, timestamp: Long): Either[PermissionError, Unit]
  }
  private case class RequireAnyOf(roles: List[Role]) extends RequireRole {
    def validate(permissions: Permissions, timestamp: Long): Either[PermissionError, Unit] =
      permissions.active(timestamp).toList.intersect(roles) match {
        case Nil =>
          Left(PermissionError(s"Doesn't have any of required roles: ${roles.map(_.prefixS).mkString(", ")}"))
        case _ =>
          Right(())
      }
  }
  private case class RequireAllOf(roles: List[Role]) extends RequireRole {
    def validate(permissions: Permissions, timestamp: Long): Either[PermissionError, Unit] = {
      val activePermissions = permissions.active(timestamp).toList
      roles.diff(activePermissions) match {
        case Nil =>
          Right(())
        case missingRoles =>
          Left(PermissionError(s"Required ${roles.map(_.prefixS).mkString(", ")} roles, missing ${missingRoles.map(_.prefixS).mkString(", ")} roles"))
      }
    }
  }
  private case class RequireSingle(role: Role) extends RequireRole {
    def validate(permissions: Permissions, timestamp: Long): Either[PermissionError, Unit] =
      Either.cond(permissions.contains(role, timestamp), (), PermissionError(s"Has no active '$role' role"))
  }
  private case class RequireNot(role: Role) extends RequireRole {
    def validate(permissions: Permissions, timestamp: Long): Either[PermissionError, Unit] = {
      Either.cond(!permissions.contains(role, timestamp), (), PermissionError(s"Sender is '$role'"))
    }
  }
  private case object RequireNone extends RequireRole {
    def validate(permissions: Permissions, timestamp: Long): Either[PermissionError, Unit] = Right(())
  }
}

object PermissionValidator {

  def apply(genesisSettings: GenesisSettings): PermissionValidator = new PermissionValidatorImpl(genesisSettings.senderRoleEnabled)

  def apply(senderRoleEnabled: Boolean): PermissionValidator = new PermissionValidatorImpl(senderRoleEnabled)
}
