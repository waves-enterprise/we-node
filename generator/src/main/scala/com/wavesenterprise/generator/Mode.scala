package com.wavesenterprise.generator

object Mode extends Enumeration {
  type Mode = Value
  val WIDE, NARROW, DYN_WIDE, MULTISIG, ORACLE, SWARM, DOCKER_CALL = Value
}
