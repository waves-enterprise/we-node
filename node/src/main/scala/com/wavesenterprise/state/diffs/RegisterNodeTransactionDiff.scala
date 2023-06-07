package com.wavesenterprise.state.diffs

import com.wavesenterprise.acl.OpType
import com.wavesenterprise.state.{Blockchain, Diff, LeaseBalance, ParticipantRegistration, Portfolio}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{RegisterNodeTransaction, ValidationError}
import com.wavesenterprise.state.AssetHolder._

case class RegisterNodeTransactionDiff(blockchain: Blockchain, height: Int) {
  import RegisterNodeTransactionDiff._

  def apply(tx: RegisterNodeTransaction): Either[ValidationError, Diff] = {
    val targetAlreadyPresent = blockchain.participantPubKey(tx.target.toAddress).isDefined

    (targetAlreadyPresent, tx.opType) match {

      case (true, OpType.Add)     => Left(GenericError(s"Account '${tx.target}' already presented in blockchain"))
      case (false, OpType.Remove) => Left(GenericError(s"Account '${tx.target}' is not present in blockchain, therefore not allowed to remove it"))
      case (true, OpType.Remove) if blockchain.networkParticipants().lengthCompare(minNetworkParticipantsCount + 1) < 0 =>
        Left(
          GenericError(s"Account '${tx.target}' cannot be removed because number of participants will be less then '$minNetworkParticipantsCount'"))
      case _ =>
        Right(
          Diff(
            height,
            tx,
            portfolios = Map(tx.sender.toAddress.toAssetHolder -> Portfolio(-tx.fee, LeaseBalance.empty, Map.empty)),
            registrations = Seq(ParticipantRegistration(tx.target.toAddress, tx.target, tx.opType))
          ))
    }
  }
}

object RegisterNodeTransactionDiff {
  val minNetworkParticipantsCount = 3
}
