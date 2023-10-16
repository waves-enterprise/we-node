package com.wavesenterprise.utx

import com.wavesenterprise.account.Address
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.network.TransactionWithSize
import com.wavesenterprise.protobuf.service.transaction.UtxSize
import com.wavesenterprise.state.{ByteStr, Diff, Portfolio}
import com.wavesenterprise.transaction._
import com.wavesenterprise.utx.UtxPool.TxWithCerts
import monix.reactive.Observable
import org.reactivestreams.Publisher

trait UtxPool extends UtxCertStorage with AutoCloseable {

  def putIfNew(tx: Transaction, maybeCerts: Option[CertChain]): Either[ValidationError, (Boolean, Diff)]

  def putIfNewWithSize(txMessage: TransactionWithSize, maybeCerts: Option[CertChain]): Either[ValidationError, (Boolean, Diff)]

  def txDiffer(tx: Transaction, maybeCerts: Option[CertChain]): Either[ValidationError, Diff]

  def forcePut(tx: TransactionWithSize, diff: Diff, maybeCerts: Option[CertChain]): Boolean

  def remove(tx: Transaction, reason: Option[String], mustBeInPool: Boolean): Unit

  def removeAll(txs: Seq[Transaction], mustBeInPool: Boolean): Unit

  def removeAll(txToError: Map[Transaction, String]): Unit

  def accountPortfolio(addr: Address): Portfolio

  def portfolio(addr: Address): Portfolio

  def all: Seq[Transaction]

  def size: UtxSize

  def lastSize: Publisher[UtxSize]

  def transactionById(transactionId: ByteStr): Option[Transaction]

  def transactionWithCertsById(transactionId: ByteStr): Option[TxWithCerts]

  def selectTransactions(predicate: Transaction => Boolean): Array[Transaction]

  def contains(transactionId: ByteStr): Boolean

  def containsInsideAtomic(transactionId: ByteStr): Boolean

  def selectOrderedTransactions(predicate: Transaction => Boolean): Array[Transaction]

  def selectTransactionsWithCerts(predicate: Transaction => Boolean): Array[TxWithCerts]

  def selectOrderedTransactionsWithCerts(predicate: Transaction => Boolean): Array[TxWithCerts]

  def confidentialContractDataUpdates: Observable[ConfidentialContractDataUpdate]

  def cleanup(): Unit
}

object UtxPool {

  case class TxWithCerts(tx: Transaction, maybeCerts: Option[CertChain])

}
