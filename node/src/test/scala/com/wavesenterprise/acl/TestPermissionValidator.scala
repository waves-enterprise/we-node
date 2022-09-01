package com.wavesenterprise.acl

import com.wavesenterprise.account.Address
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.{Transaction, ValidationError}

object TestPermissionValidator {
  def permissionValidatorNoOp(): PermissionValidator = new PermissionValidator {
    override def validatePermissionForTx(blockchain: Blockchain, tx: Transaction): Either[ValidationError.PermissionError, Unit]           = Right(())
    override def validateMiner(blockchain: Blockchain, address: Address, onTimestamp: Long): Either[ValidationError.PermissionError, Unit] = Right(())
  }
}
