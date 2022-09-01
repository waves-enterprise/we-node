package com.wavesenterprise.state

import com.wavesenterprise.mining.MiningConstraint

trait MiningConstraintsHolder {
  def restTotalConstraint: MiningConstraint
}
