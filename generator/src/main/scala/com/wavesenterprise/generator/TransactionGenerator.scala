package com.wavesenterprise.generator

import com.wavesenterprise.transaction.Transaction
import monix.eval.Task

import java.net.URL

trait TransactionGenerator extends Iterator[Iterator[Transaction]] {
  override val hasNext = true

  def initTxs: Seq[Transaction] = Seq.empty

  def afterInitTxsValidation(txs: Seq[Transaction], nodeUrl: URL): Task[_] = Task.unit
}
