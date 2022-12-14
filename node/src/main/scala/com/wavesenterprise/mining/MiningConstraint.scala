package com.wavesenterprise.mining

import cats.data.NonEmptyList
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.Transaction

trait MiningConstraint {
  def isFull: Boolean
  def isOverfilled: Boolean
  def put(blockchain: Blockchain, x: Transaction): MiningConstraint

  /**
    *
    * Overflowing critical constraint ( also if it in MultiDimensionalMiningConstraint) stops TransactionsConfirmatory,
    * while overflowing regular constraint will cause only logging of this event
    */
  def isCriticalConstraint: Boolean
  def hasOverfilledCriticalConstraint: Boolean = isCriticalConstraint && isOverfilled
}

object MiningConstraint {
  val Unlimited: MiningConstraint = new MiningConstraint {
    override def isFull: Boolean                                               = false
    override def isOverfilled: Boolean                                         = false
    override def put(blockchain: Blockchain, x: Transaction): MiningConstraint = this

    override def isCriticalConstraint: Boolean = false
  }
}

case class OneDimensionalMiningConstraint(rest: Long, estimator: TxEstimators.Fn, isCriticalConstraint: Boolean = false) extends MiningConstraint {
  override def isFull: Boolean = {
    rest < estimator.minEstimate
  }
  override def isOverfilled: Boolean = {
    rest < 0
  }
  override def put(blockchain: Blockchain, x: Transaction): OneDimensionalMiningConstraint = put(estimator(blockchain, x))
  private def put(x: Long): OneDimensionalMiningConstraint = {
    copy(rest = this.rest - x)
  }
}

case class MultiDimensionalMiningConstraint(constraints: NonEmptyList[MiningConstraint], isCriticalConstraint: Boolean = false)
    extends MiningConstraint {
  override def isFull: Boolean       = constraints.exists(_.isFull)
  override def isOverfilled: Boolean = constraints.exists(_.isOverfilled)
  override def put(blockchain: Blockchain, x: Transaction): MultiDimensionalMiningConstraint =
    MultiDimensionalMiningConstraint(constraints.map(_.put(blockchain, x)))
  override def hasOverfilledCriticalConstraint: Boolean =
    constraints.exists(constraint => constraint.hasOverfilledCriticalConstraint)
}

object MultiDimensionalMiningConstraint {
  def apply(constraint1: MiningConstraint, constraint2: MiningConstraint): MultiDimensionalMiningConstraint =
    MultiDimensionalMiningConstraint(NonEmptyList.of(constraint1, constraint2))
}
