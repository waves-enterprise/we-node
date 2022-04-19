package com.wavesenterprise.utx

import com.wavesenterprise.account.Address
import com.wavesenterprise.network.TransactionWithSize
import com.wavesenterprise.protobuf.service.transaction.UtxSize
import com.wavesenterprise.state.{ByteStr, Diff, Portfolio}
import com.wavesenterprise.transaction._
import monix.reactive.Observable

trait UtxPool extends AutoCloseable {

  def putIfNew(tx: Transaction): Either[ValidationError, (Boolean, Diff)]

  def putIfNewWithSize(txMessage: TransactionWithSize): Either[ValidationError, (Boolean, Diff)]

  def txDiffer(tx: Transaction): Either[ValidationError, Diff]

  def forcePut(tx: TransactionWithSize, diff: Diff): Boolean

  def remove(tx: Transaction, reason: Option[String], mustBeInPool: Boolean): Unit

  def removeAll(txs: Seq[Transaction], mustBeInPool: Boolean): Unit

  def removeAll(txToError: Map[Transaction, String]): Unit

  def accountPortfolio(addr: Address): Portfolio

  def portfolio(addr: Address): Portfolio

  def all: Seq[Transaction]

  def size: UtxSize

  def lastSize: Observable[UtxSize]

  def transactionById(transactionId: ByteStr): Option[Transaction]

  def selectTransactions(predicate: Transaction => Boolean): Array[Transaction]

  def contains(transactionId: ByteStr): Boolean

  def containsInsideAtomic(transactionId: ByteStr): Boolean

  def selectOrderedTransactions(predicate: Transaction => Boolean): Array[Transaction]

  def cleanup(): Unit
}
