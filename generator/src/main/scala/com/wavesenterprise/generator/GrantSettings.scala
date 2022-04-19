package com.wavesenterprise.generator

import cats.implicits._
import com.wavesenterprise.account.{AddressOrAlias, PrivateKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.acl.{PermitTransaction, PermitTransactionV1}

case class RoleOperation(role: Role, operation: OpType, dueTimestamp: Option[Long]) {
  def toPermissionOp(timestamp: Long): PermissionOp =
    PermissionOp(operation, role, timestamp, dueTimestamp)
}

case class GrantSettings(address: String, assigns: Seq[RoleOperation]) {
  def mkTransactions(sender: PrivateKeyAccount, permitTxfee: Long): Either[ValidationError, List[PermitTransaction]] = {
    assigns.toList.traverse[GrantRolesApp.MaybeOrErr, PermitTransaction] { roleOp =>
      val ts = System.currentTimeMillis()
      for {
        target <- AddressOrAlias.fromString(address).leftMap(ValidationError.fromCryptoError)
        tx     <- PermitTransactionV1.selfSigned(sender, target, ts, permitTxfee, roleOp.toPermissionOp(ts))
      } yield tx
    }
  }
}
