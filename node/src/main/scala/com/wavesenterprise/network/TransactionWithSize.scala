package com.wavesenterprise.network

import com.wavesenterprise.transaction.Transaction

case class TransactionWithSize(size: Int, tx: Transaction) extends TxWithSize
