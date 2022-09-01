package com.wavesenterprise.settings

object TestFeeUtils {
  implicit class TestFeeExt(testFees: TestFees) {
    def toFeeSettings: FeeSettings = FeeSettings.FeesEnabled(testFees.base, testFees.additional)
  }
}
