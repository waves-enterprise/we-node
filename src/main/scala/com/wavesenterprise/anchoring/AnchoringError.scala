package com.wavesenterprise.anchoring

import scala.collection.immutable
import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.Uppercase

class AnchoringException(val errorType: AnchoringErrorType) extends Exception

sealed trait AnchoringErrorType extends EnumEntry with Uppercase {
  def code: Int
  def reason: String
}

object AnchoringErrorType extends Enum[AnchoringErrorType] {

  val values: immutable.IndexedSeq[AnchoringErrorType] = findValues
  def fromStr(str: String): AnchoringErrorType = {
    withName(str.toUpperCase)
  }

  case object UnknownError extends AnchoringErrorType {
    override def code: Int      = 0
    override def reason: String = "unknown error"
  }

  case object TargetnetTransactionCreationError extends AnchoringErrorType {
    override def code: Int      = 1
    override def reason: String = "failed to create a data transaction for targetnet"
  }

  case object SendingTransactionToTargetnetError extends AnchoringErrorType {
    override def code: Int      = 2
    override def reason: String = "failed to send the transaction to targetnet"
  }

  case class TargetnetReturnWrongResponseStatus(responseStatus: Int) extends AnchoringErrorType {
    override def code: Int      = 3
    override def reason: String = s"invalid http status of response from targetnet transaction broadcast: $responseStatus"
  }

  case object TargetnetReturnWrongResponse extends AnchoringErrorType {
    override def code: Int      = 4
    override def reason: String = s"failed to parse http body of response from targetnet transaction broadcast"
  }

  case class TargetnetTxIdDifferFromSentTx(targetnetTxId: String, sentTxId: String) extends AnchoringErrorType {
    override def code: Int = 5
    override def reason: String =
      s"targetnet returned transaction with id='$targetnetTxId', but it differs from transaction that was sent id='$sentTxId'"
  }

  case object TargetnetTxInfoError extends AnchoringErrorType {
    override def code: Int      = 6
    override def reason: String = "targetnet didn't respond on transaction info request"
  }

  case object TargetnetHeightError extends AnchoringErrorType {
    override def code: Int      = 7
    override def reason: String = "failed to get current height in targetnet"
  }

  case object AnchoringTxInTargetnetDisappear extends AnchoringErrorType {
    override def code: Int      = 8
    override def reason: String = "anchoring transaction in targetnet not found after height rise await"
  }

  case object SidechainTxCreationFailed extends AnchoringErrorType {
    override def code: Int      = 9
    override def reason: String = "failed to create sidechain anchoring transaction"
  }

  case object AnchoredTxChanged extends AnchoringErrorType {
    override def code: Int = 10
    override def reason: String =
      "anchored block in sidechain has been changed during targetnet height arise await, possibly a rollback has happened"
  }
}
