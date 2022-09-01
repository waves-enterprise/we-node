package com.wavesenterprise.utils

import com.wavesenterprise.settings.{ConsensusType, NtpSettings}
import monix.execution.Scheduler

object NTPUtils {
  implicit class NTPExt(ntp: NTP.type) {
    def apply(consensusType: ConsensusType, ntp: NtpSettings)(implicit scheduler: Scheduler): NTP = {
      val ntpMode = (consensusType, ntp.fatalTimeout) match {
        case (ConsensusType.PoA | ConsensusType.CFT, Some(timeout)) => NtpMode.Sensitive(timeout)
        case (ConsensusType.PoA | ConsensusType.CFT, None) =>
          throw new Exception(s"NTP fatal-timeout not configured but required for PoA consensus.")
        case (_, timeoutOpt) => timeoutOpt.fold[NtpMode](NtpMode.Normal)(NtpMode.Sensitive)
      }
      new NTP(ntp.servers, ntpMode, ntp.requestTimeout, ntp.expirationTimeout)
    }
  }
}
