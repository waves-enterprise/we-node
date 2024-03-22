package com.wavesenterprise

import com.wavesenterprise.crypto.{CryptoInitializer, WavesCryptoContext}
import com.wavesenterprise.crypto.internals.{CryptoContext, CryptoError}
import com.wavesenterprise.lang.WavesGlobal
import com.wavesenterprise.lang.v1.BaseGlobal
import com.wavesenterprise.settings.{CrlSyncManagerSettings, CryptoSettings, PkiCryptoSettings, PkiMode}

import scala.concurrent.duration.DurationInt

trait CryptoInitializerSpec {
  CryptoInitializer.init(TestWavesCrypto(
    PkiCryptoSettings.EnabledPkiSettings(
      Set.empty,
      crlChecksEnabled = false,
      CrlSyncManagerSettings(2 hours)
    )))
}

case class TestWavesCrypto(pkiSettings: PkiCryptoSettings) extends CryptoSettings {
  override def byteId: Byte                                = 2
  override def cryptoContext: CryptoContext                = new WavesCryptoContext()
  override def rideContext: BaseGlobal                     = WavesGlobal
  override def allowedPkiModes: Set[PkiMode]               = PkiMode.values.toSet
  override def checkEnvironment: Either[CryptoError, Unit] = Right(())
}
