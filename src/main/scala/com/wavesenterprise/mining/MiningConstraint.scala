package com.wavesenterprise.mining

import cats.data.NonEmptyList
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.Transaction

trait MiningConstraint {
  def isFull: Boolean
  def isOverfilled: Boolean
  def put(blockchain: Blockchain, x: Transaction): MiningConstraint
}

object MiningConstraint {
  val Unlimited: MiningConstraint = new MiningConstraint {
    override def isFull: Boolean                                               = false
    override def isOverfilled: Boolean                                         = false
    override def put(blockchain: Blockchain, x: Transaction): MiningConstraint = this
  }
}

case class OneDimensionalMiningConstraint(rest: Long, estimator: TxEstimators.Fn) extends MiningConstraint {
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

case class MultiDimensionalMiningConstraint(constraints: NonEmptyList[MiningConstraint]) extends MiningConstraint {
  override def isFull: Boolean       = constraints.exists(_.isFull)
  override def isOverfilled: Boolean = constraints.exists(_.isOverfilled)
  override def put(blockchain: Blockchain, x: Transaction): MultiDimensionalMiningConstraint =
    MultiDimensionalMiningConstraint(constraints.map(_.put(blockchain, x)))
}

object MultiDimensionalMiningConstraint {
  def apply(constraint1: MiningConstraint, constraint2: MiningConstraint): MultiDimensionalMiningConstraint =
    MultiDimensionalMiningConstraint(NonEmptyList.of(constraint1, constraint2))
}
