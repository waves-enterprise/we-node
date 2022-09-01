package com.wavesenterprise.network

import com.wavesenterprise.transaction.Transaction

trait TxWithSize {

  def size: Int

  def tx: Transaction

}
