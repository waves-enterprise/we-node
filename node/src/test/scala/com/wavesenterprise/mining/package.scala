package com.wavesenterprise

import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.Transaction

package object mining {
  private[mining] def createConstConstraint(maxSize: Long, transactionSize: => Long) = OneDimensionalMiningConstraint(
    maxSize,
    new com.wavesenterprise.mining.TxEstimators.Fn {
      override def apply(b: Blockchain, t: Transaction) = transactionSize
      override val minEstimate                          = transactionSize
    }
  )
}
