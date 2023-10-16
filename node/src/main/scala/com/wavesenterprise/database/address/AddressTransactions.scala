package com.wavesenterprise.database.address

import com.wavesenterprise.account.Address
import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.rocksdb.MainReadOnlyDB
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.Transaction.Type

import scala.collection.SeqView

object AddressTransactions {

  def apply(db: MainReadOnlyDB,
            address: Address,
            txTypes: Set[Type],
            count: Int,
            fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]] = {
    takeTxIds(db, address, txTypes, count, fromId).map { txIds =>
      txIds.flatMap(id => db.get(Keys.transactionInfo(id))).force
    }
  }

  protected[database] def takeTxIds(db: MainReadOnlyDB,
                                    address: Address,
                                    txTypes: Set[Type],
                                    count: Int,
                                    fromId: Option[ByteStr]): Either[String, SeqView[ByteStr, Seq[_]]] = {
    validateFromId(db, fromId).map { _ =>
      takeTxIdsView(db, address, txTypes, count, fromId)
    }
  }

  private def validateFromId(db: MainReadOnlyDB, fromId: Option[ByteStr]): Either[String, Unit] = {
    fromId match {
      case None => Right(())
      case Some(fId) =>
        db.get(Keys.transactionInfo(fId)) match {
          case None    => Left(s"Transaction '$fId' does not exist")
          case Some(_) => Right(())
        }
    }
  }

  private def takeTxIdsView(db: MainReadOnlyDB,
                            address: Address,
                            txTypes: Set[Type],
                            count: Int,
                            fromId: Option[ByteStr]): SeqView[ByteStr, Seq[_]] = {
    db.get(Keys.addressId(address)).fold[SeqView[ByteStr, Seq[_]]](Seq.empty[ByteStr].view) { addressId =>
      val txIds = for {
        seqNr          <- (db.get(Keys.addressTransactionSeqNr(addressId)) to 1 by -1).view
        (txType, txId) <- db.get(Keys.addressTransactionIds(addressId, seqNr))
        if txTypes.isEmpty || txTypes.contains(txType.toByte)
      } yield txId

      takeAfterTx(txIds, fromId).take(count)
    }
  }

  private def takeAfterTx(s: SeqView[ByteStr, Seq[_]], fromId: Option[ByteStr]): SeqView[ByteStr, Seq[_]] = {
    fromId match {
      case None     => s
      case Some(id) => s.dropWhile(_ != id).drop(1)
    }
  }
}
