package com.wavesenterprise.consensus

import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.{PermissionOp, Permissions, Role}
import com.wavesenterprise.state.Blockchain

class ContractValidatorPool(val validatorPermissions: Map[Address, Permissions]) {

  /** All addresses that have ever been validators. May include currently inactive validators. */
  def suggestedValidators: Set[Address] = validatorPermissions.keySet

  def currentValidatorSet(timestamp: Long): Set[Address] =
    (for {
      (address, permissions) <- validatorPermissions
      _                      <- permissions.lastActive(Role.ContractValidator, timestamp)
    } yield address).toSet

  def remove(address: Address, permissionOp: PermissionOp): ContractValidatorPool =
    if (permissionOp.role == Role.ContractValidator) {
      validatorPermissions.get(address) match {
        case Some(permissions) =>
          val updatePermissions = Permissions(permissions.value - permissionOp)
          ContractValidatorPool(validatorPermissions.updated(address, updatePermissions))
        case None =>
          this
      }
    } else {
      this
    }

  def update(newPermissions: Map[Address, Permissions]): ContractValidatorPool =
    ContractValidatorPool(validatorPermissions ++ newPermissions)
}

object ContractValidatorPool {

  def apply(validatorPermissions: Map[Address, Permissions]): ContractValidatorPool =
    new ContractValidatorPool(validatorPermissions)

  val empty: ContractValidatorPool =
    ContractValidatorPool(Map.empty[Address, Permissions])
}

class ContractValidatorsProvider(state: Blockchain) {
  def get(): Set[Address] = state.lastBlockContractValidators
}
