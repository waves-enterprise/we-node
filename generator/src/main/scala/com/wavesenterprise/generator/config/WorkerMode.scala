package com.wavesenterprise.generator.config

sealed trait WorkerMode

object WorkerMode {
  case object Stop     extends WorkerMode
  case object Continue extends WorkerMode
  case object Freeze   extends WorkerMode
}
