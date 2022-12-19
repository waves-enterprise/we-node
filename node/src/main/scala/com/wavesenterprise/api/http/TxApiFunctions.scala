package com.wavesenterprise.api.http

import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.lease.LeaseCancelTransaction
import play.api.libs.json._

trait TxApiFunctions {

  def blockchain: Blockchain

  protected[api] def txToExtendedJson(tx: Transaction): JsObject = {
    import com.wavesenterprise.transaction.lease.LeaseTransaction
    tx match {
      case lease: LeaseTransaction =>
        import com.wavesenterprise.transaction.lease.LeaseTransactionStatus._
        lease.json() ++ Json.obj("status" -> (if (blockchain.leaseDetails(lease.id()).exists(_.isActive)) Active else Canceled))
      case leaseCancel: LeaseCancelTransaction =>
        leaseCancel.json() ++ Json.obj("lease" -> blockchain.transactionInfo(leaseCancel.leaseId).map(_._2.json()).getOrElse[JsValue](JsNull))
      case t => t.json()
    }
  }

  protected[api] def blockWithExtendedTxInfo(block: Block): JsObject = {
    val transactionData = block.transactionData
    BlockHeader.json(block.blockHeader, block.bytes().length) ++
      Json.obj("fee" -> block.blockFee()) ++
      Json.obj("transactions" -> JsArray(transactionData.map(txToExtendedJson)))
  }
}
