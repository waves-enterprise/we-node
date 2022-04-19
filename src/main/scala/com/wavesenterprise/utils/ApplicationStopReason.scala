package com.wavesenterprise.utils

sealed abstract class ApplicationStopReason(val code: Int)
case object Default              extends ApplicationStopReason(1)
case object UnsupportedFeature   extends ApplicationStopReason(38)
case object PasswordNotSpecified extends ApplicationStopReason(61)
case object NotEnoughDiskSpace   extends ApplicationStopReason(1)
